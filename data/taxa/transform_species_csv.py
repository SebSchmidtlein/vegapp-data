#!/usr/bin/env python3
"""
Species CSV Transformation Tool
Converts old format species CSV files to new format with 64-bit IDs
Supports both individual CSV/TXT files and ZIP archives containing CSV/TXT files

Usage:
    python transform_species_csv.py <input_file> [output_file]
    
If input is a ZIP file:
- Extracts the CSV or TXT file inside
- Transforms it to new format
- Creates a new ZIP with "_transformed" suffix
    
If output_file is not specified, input file will be overwritten.

Old format: SPECIES_NR;NAME;GENUS;SPECIES;AUTHOR;SYNONYM;VALID_NR;VALID_NAME;SECUNDUM
New format: SPECIES_NR;NAME;GENUS;SPECIES;SYNONYM;VALID_NR;VALID_NAME;PARENT_NR;PARENT_NAME

Changes:
- SPECIES_LU_VERSION → SPECIES_LU
- Removes AUTHOR and SECUNDUM columns
- Converts all IDs to 64-bit integers (excluding upper 10 billion)
- Adds PARENT_NR and PARENT_NAME columns for hierarchical relationships
- Maintains synonym relationships via VALID_NR mapping
- Automatically converts WAHR/FALSCH to TRUE/FALSE
"""

import random
import sys
import os
import zipfile
import tempfile
from pathlib import Path
from typing import Dict, List, Tuple

def generate_64bit_id(exclude_upper_10_billion: bool = True) -> int:
    """Generate a 64-bit integer ID, optionally excluding upper 10 billion range"""
    if exclude_upper_10_billion:
        # Use range from 1 to (2^64 - 10^10 - 1) to exclude upper 10 billion
        max_val = 2**64 - 10**10 - 1
        return random.randint(1, max_val)
    else:
        return random.randint(1, 2**64 - 1)

def parse_csv_file(file_path: str) -> Tuple[List[str], List[List[str]]]:
    """Parse CSV file and return metadata lines and data rows"""
    metadata_lines = []
    data_rows = []
    header_found = False
    
    # Try different encodings for files with special characters
    encodings_to_try = ['utf-8-sig', 'utf-8', 'latin1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings_to_try:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                metadata_lines = []
                data_rows = []
                header_found = False
                
                for line_num, line in enumerate(f):
                    line = line.strip()
                    if not line:
                        continue
                        
                    # Check if this is a metadata line (contains semicolon but not the header)
                    if ';' in line and not header_found:
                        # Check if this looks like the data header
                        if 'SPECIES_NR' in line and 'NAME' in line and 'GENUS' in line:
                            header_found = True
                            # This is the header - we'll replace it
                            continue
                        else:
                            # This is metadata
                            metadata_lines.append(line)
                    elif header_found:
                        # This is data
                        parts = line.split(';')
                        if len(parts) >= 8:  # Ensure we have enough columns
                            data_rows.append(parts)
                
                print(f"Successfully parsed file using {encoding} encoding")
                break
                
        except UnicodeDecodeError:
            if encoding == encodings_to_try[-1]:  # Last encoding attempt
                raise ValueError(f"Could not decode file with any of the tried encodings: {encodings_to_try}")
            continue
        except Exception as e:
            raise ValueError(f"Error parsing file: {e}")
    
    return metadata_lines, data_rows

def transform_species_csv(input_file: str, output_file: str = None):
    """Transform species CSV from old format to new format"""
    
    if output_file is None:
        output_file = input_file
    
    print(f"Transforming {input_file}...")
    
    # Parse the input file
    metadata_lines, data_rows = parse_csv_file(input_file)
    
    print(f"Found {len(metadata_lines)} metadata lines and {len(data_rows)} data rows")
    
    # Update metadata
    new_metadata = []
    for line in metadata_lines:
        if line.startswith('SPECIES_LU_VERSION'):
            # Convert SPECIES_LU_VERSION to SPECIES_LU but keep the original value
            parts = line.split(';', 1)  # Split only on first semicolon
            if len(parts) == 2:
                new_metadata.append(f'SPECIES_LU;{parts[1]}')
            else:
                # Fallback if format is unexpected
                new_metadata.append(line.replace('SPECIES_LU_VERSION', 'SPECIES_LU'))
        else:
            new_metadata.append(line)
    
    # Create ID mappings
    old_to_new_id = {}
    used_ids = set()
    
    # First pass: collect all unique old IDs and generate new ones
    for row in data_rows:
        if len(row) >= 7:  # Minimum columns needed
            try:
                old_species_nr = int(row[0])
                old_valid_nr = int(row[6]) if len(row) > 6 else old_species_nr
                
                # Generate new ID for species if not exists
                if old_species_nr not in old_to_new_id:
                    new_id = generate_64bit_id()
                    while new_id in used_ids:
                        new_id = generate_64bit_id()
                    old_to_new_id[old_species_nr] = new_id
                    used_ids.add(new_id)
                
                # Generate new ID for valid species if not exists
                if old_valid_nr not in old_to_new_id:
                    new_id = generate_64bit_id()
                    while new_id in used_ids:
                        new_id = generate_64bit_id()
                    old_to_new_id[old_valid_nr] = new_id
                    used_ids.add(new_id)
                    
            except (ValueError, IndexError) as e:
                print(f"Warning: Skipping invalid row: {row[:3]}... - {e}")
                continue
    
    print(f"Generated {len(old_to_new_id)} new 64-bit IDs")
    
    # Transform data rows
    transformed_rows = []
    for row in data_rows:
        if len(row) >= 8:  # Need at least 8 columns for old format
            try:
                # Parse old format: SPECIES_NR;NAME;GENUS;SPECIES;AUTHOR;SYNONYM;VALID_NR;VALID_NAME;[SECUNDUM]
                old_species_nr = int(row[0])
                name = row[1]
                genus = row[2] 
                species = row[3]
                # Skip AUTHOR (row[4])
                synonym = row[5]  # FALSE/TRUE or FALSCH/WAHR
                old_valid_nr = int(row[6])
                valid_name = row[7]
                # Skip SECUNDUM (row[8] if exists)
                
                # Convert to new format
                new_species_nr = old_to_new_id[old_species_nr]
                new_valid_nr = old_to_new_id[old_valid_nr]
                
                # PARENT_NR and PARENT_NAME are for taxonomic hierarchy, not synonyms
                # For flat species lists (like this example), these should be empty
                # Only populate if there's a true taxonomic parent-child relationship
                parent_nr = ""
                parent_name = ""
                
                # Create new row: SPECIES_NR;NAME;GENUS;SPECIES;SYNONYM;VALID_NR;VALID_NAME;PARENT_NR;PARENT_NAME
                new_row = [
                    str(new_species_nr),
                    name,
                    genus,
                    species,
                    synonym,
                    str(new_valid_nr),
                    valid_name,
                    str(parent_nr) if parent_nr else "",
                    parent_name
                ]
                
                transformed_rows.append(new_row)
                
            except (ValueError, IndexError) as e:
                print(f"Warning: Skipping invalid row: {row[:3]}... - {e}")
                continue
    
    # Write output file
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        # Write metadata
        for line in new_metadata:
            f.write(line + '\n')
        
        # Write new header
        f.write('SPECIES_NR;NAME;GENUS;SPECIES;SYNONYM;VALID_NR;VALID_NAME;PARENT_NR;PARENT_NAME\n')
        
        # Write data
        for row in transformed_rows:
            f.write(';'.join(row) + '\n')
    
    # Convert German boolean values to English if present
    with open(output_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Replace German boolean values with English ones
    content = content.replace(';WAHR;', ';TRUE;')
    content = content.replace(';FALSCH;', ';FALSE;')
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"Transformation complete. Output written to {output_file}")
    print(f"Transformed {len(transformed_rows)} species records")
    
    # Show some statistics
    synonyms = sum(1 for row in transformed_rows if row[4] in ["WAHR", "TRUE"])
    valid_species = len(transformed_rows) - synonyms
    print(f"Valid species: {valid_species}, Synonyms: {synonyms}")
    print("German boolean values (WAHR/FALSCH) converted to English (TRUE/FALSE)")

def is_zip_file(file_path: str) -> bool:
    """Check if the file is a ZIP archive"""
    return file_path.lower().endswith('.zip')

def extract_csv_from_zip(zip_path: str) -> str:
    """Extract the first species data file found in a ZIP archive to a temporary file"""
    print(f"Opening ZIP file: {zip_path}")
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Find species data files (CSV or TXT)
            data_files = [f for f in zip_ref.namelist() if f.lower().endswith(('.csv', '.txt'))]
            
            if not data_files:
                raise ValueError("No CSV or TXT files found in the ZIP archive")
            
            if len(data_files) > 1:
                print(f"Found {len(data_files)} data files, using the first one: {data_files[0]}")
            
            data_filename = data_files[0]
            print(f"Extracting: {data_filename}")
            
            # Extract to temporary file
            temp_dir = tempfile.mkdtemp()
            temp_csv_path = os.path.join(temp_dir, os.path.basename(data_filename))
            
            # Try standard extraction first
            try:
                with zip_ref.open(data_filename) as source:
                    with open(temp_csv_path, 'wb') as target:
                        target.write(source.read())
                print(f"Successfully extracted: {data_filename}")
                
            except (zipfile.BadZipFile, OSError) as e:
                print(f"Standard extraction failed ({e}), trying alternative method...")
                
                # Try extractall as fallback for problematic ZIP files
                try:
                    zip_ref.extractall(temp_dir)
                    extracted_files = [f for f in os.listdir(temp_dir) if f.lower().endswith(('.csv', '.txt'))]
                    if extracted_files:
                        temp_csv_path = os.path.join(temp_dir, extracted_files[0])
                        print(f"Alternative extraction successful: {extracted_files[0]}")
                    else:
                        raise ValueError("No data files found after alternative extraction")
                        
                except Exception as e2:
                    raise ValueError(f"Both extraction methods failed. ZIP file may be corrupted or contain special characters. Standard error: {e}, Alternative error: {e2}")
            
            # Verify the extracted file is readable
            try:
                with open(temp_csv_path, 'r', encoding='utf-8-sig') as test_file:
                    first_line = test_file.readline()
                    if not first_line.strip():
                        raise ValueError("Extracted file appears to be empty")
                print(f"File verification successful - contains data")
                
            except UnicodeDecodeError:
                print("Warning: UTF-8 encoding failed, trying with different encodings...")
                # Try common alternative encodings
                for encoding in ['latin1', 'cp1252', 'iso-8859-1']:
                    try:
                        with open(temp_csv_path, 'r', encoding=encoding) as test_file:
                            test_file.readline()
                        print(f"File readable with {encoding} encoding")
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    print("Warning: Could not determine file encoding, proceeding anyway...")
            
            return temp_csv_path
            
    except zipfile.BadZipFile:
        raise ValueError("File is not a valid ZIP archive or is severely corrupted")
    except Exception as e:
        raise ValueError(f"Error reading ZIP file: {e}")

def create_transformed_zip(original_zip_path: str, transformed_csv_path: str, original_data_name: str, output_zip_path: str = None):
    """Create a new ZIP file with the transformed data file"""
    # Create output ZIP name
    if output_zip_path is None:
        # No custom output specified, add "_transformed" suffix
        zip_path = Path(original_zip_path)
        output_zip_path = zip_path.with_name(f"{zip_path.stem}_transformed{zip_path.suffix}")
    
    print(f"Creating transformed ZIP: {output_zip_path}")
    
    with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_ref:
        zip_ref.write(transformed_csv_path, original_data_name)
    
    print(f"Transformation complete! New ZIP saved as: {output_zip_path}")
    return str(output_zip_path)

def process_zip_file(zip_path: str, output_path: str = None):
    """Process a ZIP file containing a species data file (CSV or TXT)"""
    try:
        # Extract data file from ZIP
        temp_csv_path = extract_csv_from_zip(zip_path)
        
        # Get original data filename from ZIP
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            data_files = [f for f in zip_ref.namelist() if f.lower().endswith(('.csv', '.txt'))]
            original_data_name = data_files[0]
        
        # Transform the data file
        temp_output_path = temp_csv_path + "_transformed"
        transform_species_csv(temp_csv_path, temp_output_path)
        
        # Create output ZIP
        if output_path:
            # User specified output path - use it exactly as provided
            create_transformed_zip(zip_path, temp_output_path, original_data_name, output_path)
        else:
            # Default: create ZIP with "_transformed" suffix
            create_transformed_zip(zip_path, temp_output_path, original_data_name)
        
        # Cleanup temporary files
        os.unlink(temp_csv_path)
        os.unlink(temp_output_path)
        
    except Exception as e:
        print(f"Error processing ZIP file: {e}")
        sys.exit(1)

def main():
    if len(sys.argv) < 2:
        print("Usage: python transform_species_csv.py <input_file> [output_file]")
        print("Supports both CSV/TXT files and ZIP archives containing CSV/TXT files")
        print("If output_file is not specified:")
        print("  - CSV/TXT files will be overwritten")
        print("  - ZIP files will create a new ZIP with '_transformed' suffix")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    if not os.path.exists(input_file):
        print(f"Error: Input file {input_file} not found")
        sys.exit(1)
    
    # Check if input is a ZIP file
    if is_zip_file(input_file):
        print("Detected ZIP file - processing ZIP archive")
        process_zip_file(input_file, output_file)
    else:
        print("Detected CSV/TXT file - processing directly")
        transform_species_csv(input_file, output_file)

if __name__ == "__main__":
    main()
