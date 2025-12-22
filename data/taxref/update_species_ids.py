#!/usr/bin/env python3
"""
Script to extract species numbers from zip files and update ids.txt

This script:
1. Opens all zip files in the current directory
2. Finds the species list file in each zip (assumes one file per zip)
3. Reads the first column after skipping 3 header lines
4. Adds the numbers to ids.txt (creates it if it doesn't exist)
"""

import os
import zipfile
import csv
from pathlib import Path

def process_zip_files():
    """Process all zip files in the current directory and extract species numbers."""
    current_dir = Path('.')
    species_numbers = set()
    species_sources = {}  # Track which file each species number came from
    duplicates_found = {}  # Track duplicates: {species_number: [file1, file2, ...]}
    
    # Read existing ids.txt if it exists
    ids_file = current_dir / 'ids.txt'
    if ids_file.exists():
        with open(ids_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and line.isdigit():
                    species_numbers.add(line)
                    species_sources[line] = "existing ids.txt"
        print(f"Loaded {len(species_numbers)} existing species IDs from ids.txt")
    
    # Process all zip files
    zip_files = list(current_dir.glob('*.zip'))
    print(f"Found {len(zip_files)} zip files to process")
    
    for zip_path in zip_files:
        print(f"Processing {zip_path.name}...")
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Get list of files in the zip
                file_list = zip_ref.namelist()
                print(f"  Files in zip: {file_list}")
                
                if not file_list:
                    print(f"  Warning: {zip_path.name} is empty")
                    continue
                
                # Look for species.txt first, then fall back to first file
                species_file = None
                for f in file_list:
                    if 'species' in f.lower() and f.endswith('.txt'):
                        species_file = f
                        break
                
                if not species_file:
                    species_file = file_list[0]
                
                print(f"  Reading species file: {species_file}")
                
                # Read the species file
                with zip_ref.open(species_file) as f:
                    # Try to detect encoding
                    content = f.read()
                    try:
                        text_content = content.decode('utf-8')
                    except UnicodeDecodeError:
                        try:
                            text_content = content.decode('latin-1')
                        except UnicodeDecodeError:
                            text_content = content.decode('utf-8', errors='ignore')
                    
                    # Split into lines and skip first 3 header lines
                    lines = text_content.strip().split('\n')
                    print(f"  Total lines in file: {len(lines)}")
                    
                    # Show first few lines for debugging
                    print("  First 5 lines:")
                    for i, line in enumerate(lines[:5]):
                        print(f"    Line {i+1}: {line[:100]}")
                    
                    if len(lines) <= 3:
                        print(f"  Warning: {species_file} has {len(lines)} lines, expected more than 3")
                        continue
                    
                    data_lines = lines[3:]  # Skip first 3 header lines
                    print(f"  Processing {len(data_lines)} data lines")
                    
                    # Show first few data lines
                    print("  First 3 data lines (after skipping headers):")
                    for i, line in enumerate(data_lines[:3]):
                        print(f"    Data line {i+1}: {line[:100]}")
                    
                    # Process each data line
                    new_numbers = 0
                    sample_extractions = []
                    
                    for line_num, line in enumerate(data_lines, start=4):  # Start at 4 since we skipped 3 header lines
                        line = line.strip()
                        if not line:
                            continue
                        
                        # Try to parse as semicolon-separated values to get first column
                        try:
                            # Split by semicolon since data uses ';' as separator
                            fields = line.split(';')
                            
                            if fields and fields[0].strip():
                                first_column = fields[0].strip()
                                
                                # Store first few extractions for debugging
                                if len(sample_extractions) < 5:
                                    sample_extractions.append(f"'{first_column}' (numeric: {first_column.isdigit()})")
                                
                                # Check if it's a valid species number (numeric)
                                if first_column.isdigit():
                                    if first_column not in species_numbers:
                                        species_numbers.add(first_column)
                                        species_sources[first_column] = zip_path.name
                                        new_numbers += 1
                                    else:
                                        # Found a duplicate - track it
                                        if first_column not in duplicates_found:
                                            duplicates_found[first_column] = [species_sources[first_column]]
                                        duplicates_found[first_column].append(zip_path.name)
                                else:
                                    # Skip non-numeric entries (might be headers or comments)
                                    continue
                        
                        except (ValueError, IndexError) as e:
                            print(f"  Warning: Could not parse line {line_num}: {line[:50]}... - {e}")
                            continue
                    
                    print(f"  Sample extractions from first column: {sample_extractions}")
                    print(f"  Added {new_numbers} new species numbers from {species_file}")
        
        except zipfile.BadZipFile:
            print(f"  Error: {zip_path.name} is not a valid zip file")
        except Exception as e:
            print(f"  Error processing {zip_path.name}: {e}")
    
    # Report duplicates found
    if duplicates_found:
        print(f"\n⚠️  DUPLICATES DETECTED ⚠️")
        print(f"Found {len(duplicates_found)} species numbers that appear in multiple files:")
        print("-" * 80)
        for species_number, sources in duplicates_found.items():
            print(f"Species {species_number} found in: {' → '.join(sources)}")
        print("-" * 80)
    
    # Write updated ids.txt
    if species_numbers:
        # Sort the numbers for consistent output
        sorted_numbers = sorted(species_numbers, key=int)
        
        with open(ids_file, 'w', encoding='utf-8') as f:
            for number in sorted_numbers:
                f.write(f"{number}\n")
        
        print(f"\nUpdated ids.txt with {len(sorted_numbers)} total species IDs")
    else:
        print("\nNo species numbers found to write to ids.txt")

if __name__ == "__main__":
    print("Species ID Extractor")
    print("===================")
    process_zip_files()
    print("\nDone!")
