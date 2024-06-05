import os
import re

def rename_files_in_current_directory():
    # Get the current directory
    directory = os.path.dirname(os.path.abspath(__file__))
    
    # Regular expression to match the pattern "DIGIT_"
    pattern = re.compile(r'^\d+_')

    # Iterate over all files in the directory
    for filename in os.listdir(directory):
        # Check if the file is a JSON or TEXT file
        if filename.endswith('.json') or filename.endswith('.txt'):
            # Find the new filename by removing the preceding digit and underscore
            new_filename = pattern.sub('', filename)
            # Construct full file paths
            old_file_path = os.path.join(directory, filename)
            new_file_path = os.path.join(directory, new_filename)
            
            # Check if the new file name already exists
            if os.path.exists(new_file_path):
                # Ensure we are not deleting files that don't match the renaming pattern
                if pattern.match(filename):
                    print(f'Duplicate found. Deleting: {old_file_path}')
                    os.remove(old_file_path)
            else:
                # Rename the file
                os.rename(old_file_path, new_file_path)
                print(f'Renamed: {filename} -> {new_filename}')

# Run the function
rename_files_in_current_directory()
