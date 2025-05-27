#!/bin/bash

# Strict mode
# set -e: exit immediately if a command exits with a non-zero status.
# set -u: treat unset variables as an error when substituting.
# set -o pipefail: the return value of a pipeline is the status of
#                  the last command to exit with a non-zero status,
#                  or zero if no command exited with a non-zero status.
set -euo pipefail

# --- Configuration & Arguments ---
original_file="${1:-}" # Default to empty string if not provided, for early check
new_file="${2:-}"
timestamp_pattern="${3:-}"
header_lines_str="${4:-}" # Use a different name initially to validate as string

tmp_original_file="" # Initialize for cleanup trap

# --- Functions ---
usage() {
  echo "Usage: $0 <original_file> <new_file> <timestamp_pattern> <num_header_lines>" >&2
  echo "Example: $0 data.csv data_legacy.csv '2025-01-01 00:00:00' 4" >&2
  exit 1
}

cleanup() {
  # This function is called on EXIT (normal or error).
  # It removes the temporary file if it was created and still exists.
  if [ -n "$tmp_original_file" ] && [ -f "$tmp_original_file" ]; then
    # echo "Cleaning up temporary file: $tmp_original_file" >&2 # Optional: for debugging
    rm -f "$tmp_original_file"
  fi
}
# Register the cleanup function to be called when the script exits.
trap cleanup EXIT

# --- Input Validation ---
if [ "$#" -ne 4 ]; then
  echo "Error: Incorrect number of arguments." >&2
  usage
fi

if [ ! -f "$original_file" ]; then
  echo "Error: Original file '$original_file' not found." >&2
  exit 1
fi

# Validate header_lines_str is a non-negative integer
if ! [[ "$header_lines_str" =~ ^[0-9]+$ ]]; then
  echo "Error: Number of header lines ('$header_lines_str') must be a non-negative integer." >&2
  exit 1
fi
# Convert to a numeric variable after validation
header_lines=$((header_lines_str))

# --- Part 1: Create the new_file ---

# 1.1. Copy header to new_file
# If header_lines is 0, head -n 0 outputs nothing, which is correct.
if ! head -n "$header_lines" "$original_file" > "$new_file"; then
    # This error is unlikely with basic head usage but good to have
    echo "Error: Failed to copy header from '$original_file' to '$new_file'." >&2
    exit 1
fi
echo "Header (lines 1-$header_lines) from '$original_file' copied to '$new_file'."

# 1.2. Find the line number of the first occurrence of the timestamp
timestamp_line_num_output=$(grep -n -m 1 -F -e "$timestamp_pattern" "$original_file" || true)

if [ -z "$timestamp_line_num_output" ]; then
  echo "Error: Timestamp pattern '$timestamp_pattern' not found in '$original_file'." >&2
  echo "'$new_file' currently contains only the header. Processing stopped." >&2
  exit 1
fi
timestamp_line_num=$(echo "$timestamp_line_num_output" | cut -d: -f1)
echo "Timestamp pattern '$timestamp_pattern' first found at line $timestamp_line_num in '$original_file'."

# 1.3. Append data from original_file (after header, up to *BEFORE* timestamp line) to new_file
if [ "$timestamp_line_num" -gt "$header_lines" ]; then
  start_data_copy_for_new_file=$((header_lines + 1))
  # Data for new_file should end at the line *before* the timestamp line
  end_data_copy_for_new_file=$((timestamp_line_num - 1))

  # Check if there are any lines to copy in this range
  if [ "$end_data_copy_for_new_file" -ge "$start_data_copy_for_new_file" ]; then
    if ! sed -n "${start_data_copy_for_new_file},${end_data_copy_for_new_file}p" "$original_file" >> "$new_file"; then
        echo "Error: Failed to append data (lines $start_data_copy_for_new_file-$end_data_copy_for_new_file from '$original_file') to '$new_file'." >&2
        exit 1
    fi
    echo "Data from line $start_data_copy_for_new_file to $end_data_copy_for_new_file of '$original_file' appended to '$new_file'."
  else
    # This case occurs if the timestamp line is immediately after the header.
    # (e.g., header=4, timestamp=5 -> end_data_copy=4, start_data_copy=5. No lines to copy)
    echo "No data lines to append to '$new_file' between header (ends line $header_lines) and the line before the timestamp (timestamp at line $timestamp_line_num)."
  fi
else
  # Timestamp is within or before the header.
  echo "Timestamp pattern found at or before specified header line count ($header_lines lines, found at line $timestamp_line_num)." >&2
  echo "'$new_file' will only contain the header data. No subsequent data lines appended." >&2
fi

# --- Part 2: Modify the original_file ---
# This part remains the same: original_file should keep the timestamp line,
# and delete lines between the header and *before* the timestamp line.

# Lines to delete from original_file: after header, up to *before* the timestamp line.
# Calculated range for deletion: (header_lines + 1) to (timestamp_line_num - 1)
start_delete_line=$((header_lines + 1))
end_delete_line=$((timestamp_line_num - 1))

if [ "$timestamp_line_num" -gt "$header_lines" ] && [ "$end_delete_line" -ge "$start_delete_line" ]; then
  tmp_original_file=$(mktemp)
  if [ -z "$tmp_original_file" ]; then
      echo "Error: Failed to create temporary file name." >&2
      exit 1
  fi

  if sed "${start_delete_line},${end_delete_line}d" "$original_file" > "$tmp_original_file"; then
    if mv "$tmp_original_file" "$original_file"; then
      echo "Lines $start_delete_line-$end_delete_line deleted from '$original_file'."
      tmp_original_file=""
    else
      echo "Error: Failed to move temporary file ('$tmp_original_file') to overwrite '$original_file'." >&2
      exit 1
    fi
  else
    echo "Error: sed command failed to process '$original_file' into temporary file '$tmp_original_file'." >&2
    exit 1
  fi
else
  echo "No data lines to delete from '$original_file' (calculated deletion range: $start_delete_line to $end_delete_line based on timestamp at line $timestamp_line_num and $header_lines header lines)."
fi

# --- Part 2.1: Modify permission on the new original_file and ---
# This part correct the permission on the new generated original_file
# and change the owner to ftpuser:ftpuser

if sudo chmod 670 "${original_file}"; then 
  echo "$original_file gets rw-rwx--- permission" >&2
else 
  echo "Cannot apply chmod to $original_file" >&2
  exit 1
fi

if sudo chown ftpuser:ftpuser "${original_file}"; then 
  echo "$original_file now belongs to ftpuser:ftpuser" >&2
else 
  echo "Cannot apply chown to $original_file" >&2
  exit 1
fi

if sudo chmod 670 "${new_file}"; then 
  echo "$original_file gets rw-rwx--- permission" >&2
else 
  echo "Cannot apply chmod to $original_file" >&2
  exit 1
fi

if sudo chown ubuntu:ubuntu "${new_file}"; then 
  echo "$original_file now belongs to ftpuser:ftpuser" >&2
else 
  echo "Cannot apply chown to $original_file" >&2
  exit 1
fi

echo "Script completed successfully."
