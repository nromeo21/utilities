#!/bin/bash

# Script to process JSONLines files with dot notation path extraction
# Supports local files, S3 URIs, and stdin/stdout with streaming
# Usage: ./process_jsonl_s3.sh "dot.path" input_file_or_s3_uri output_file_or_s3_uri
# Use "-" for stdin/stdout

set -euo pipefail

# Check arguments
if [ $# -lt 3 ] || [ $# -gt 4 ]; then
    echo "Usage: $0 \"dot.path\" input_file_or_s3_uri output_file_or_s3_uri [kms_key_id]" >&2
    echo "Use \"-\" for stdin/stdout" >&2
    echo "Supports local files (.gz), S3 URIs (s3://), and stdin/stdout" >&2
    echo "KMS key is optional and only used for S3 output (can be key ID, ARN, or alias)" >&2
    echo "Examples:" >&2
    echo "  $0 \"data.tags\" s3://bucket/input.jsonl.gz s3://bucket/output.jsonl.gz" >&2
    echo "  $0 \"user.id\" local.jsonl.gz s3://bucket/output.jsonl arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012" >&2
    echo "  $0 \"items\" s3://bucket/input.jsonl s3://bucket/output.jsonl.gz alias/my-key" >&2
    echo "  $0 \"events\" s3://bucket/input.jsonl - " >&2
    exit 1
fi

DOT_PATH="$1"
INPUT_FILE="$2"
OUTPUT_FILE="$3"
KMS_KEY_ID="${4:-}"

# Function to check if string is S3 URI
is_s3_uri() {
    local uri="$1"
    [[ "$uri" =~ ^s3:// ]]
}

# Function to determine if file/URI is gzipped
is_gzipped() {
    local file="$1"
    [[ "$file" == *.gz ]] || [[ "$file" == *.gzip ]]
}

# Function to check AWS CLI availability
check_aws_cli() {
    if ! command -v aws &> /dev/null; then
        echo "Error: AWS CLI is required for S3 operations but not found" >&2
        echo "Please install AWS CLI: https://aws.amazon.com/cli/" >&2
        exit 1
    fi
}

# Function to read input
read_input() {
    if [ "$INPUT_FILE" = "-" ]; then
        cat
    elif is_s3_uri "$INPUT_FILE"; then
        check_aws_cli
        if is_gzipped "$INPUT_FILE"; then
            aws s3 cp "$INPUT_FILE" - | zcat
        else
            aws s3 cp "$INPUT_FILE" -
        fi
    elif is_gzipped "$INPUT_FILE"; then
        zcat "$INPUT_FILE"
    else
        cat "$INPUT_FILE"
    fi
}

# Function to write output
write_output() {
    if [ "$OUTPUT_FILE" = "-" ]; then
        cat
    elif is_s3_uri "$OUTPUT_FILE"; then
        check_aws_cli
        
        # Build AWS S3 CP command with optional KMS encryption
        local aws_cmd="aws s3 cp - \"$OUTPUT_FILE\""
        
        # Add KMS encryption if key is provided
        if [ -n "$KMS_KEY_ID" ]; then
            aws_cmd="$aws_cmd --sse aws:kms --sse-kms-key-id \"$KMS_KEY_ID\""
        fi
        
        if is_gzipped "$OUTPUT_FILE"; then
            gzip | eval "$aws_cmd"
        else
            eval "$aws_cmd"
        fi
    elif is_gzipped "$OUTPUT_FILE"; then
        gzip > "$OUTPUT_FILE"
    else
        cat > "$OUTPUT_FILE"
    fi
}

# Main processing function
main() {
    local jq_filter
    
    # Create jq filter that handles the dot path extraction and array explosion
    read -r -d '' jq_filter << 'EOF' || true
def get_path_value(path_str):
  path_str | split(".") | map(tonumber? // .) as $path_array |
  getpath($path_array);

def delete_path(path_str):
  path_str | split(".") | map(tonumber? // .) as $path_array |
  delpaths([$path_array]);

def path_exists(path_str):
  path_str | split(".") | map(tonumber? // .) as $path_array |
  try (getpath($path_array) != null) catch false;

# Main processing logic
if path_exists($dot_path) then
  get_path_value($dot_path) as $value |
  if ($value | type) == "array" then
    # Array case: explode each element into separate records
    $value[] as $item | 
    delete_path($dot_path) | 
    .new_id = $item
  else
    # Single value case: rename to new_id
    delete_path($dot_path) | 
    .new_id = $value
  end
else
  # Path doesn't exist, return unchanged
  .
end
EOF

    # Process the JSONLines file with the dot path as a variable
    read_input | jq -c --arg dot_path "$DOT_PATH" "$jq_filter" | write_output
}

# Validate AWS credentials if S3 is being used
validate_aws_access() {
    if is_s3_uri "$INPUT_FILE" || is_s3_uri "$OUTPUT_FILE"; then
        check_aws_cli
        
        # Quick AWS credentials check
        if ! aws sts get-caller-identity &> /dev/null; then
            echo "Error: AWS credentials not configured or invalid" >&2
            echo "Please run 'aws configure' or set AWS environment variables" >&2
            exit 1
        fi
    fi
}

# Run validation and main function
validate_aws_access
main