import os
import sys

def clean_own_directory():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    extensions = ['.log', '.json']
    deleted_files = []

    for filename in os.listdir(script_dir):
        if any(filename.endswith(ext) for ext in extensions):
            file_path = os.path.join(script_dir, filename)
            try:
                os.remove(file_path)
                deleted_files.append(file_path)
            except Exception as e:
                print(f"Failed to delete {file_path}: {e}")

    if deleted_files:
        print("Deleted files:")
        for f in deleted_files:
            print(f"  - {f}")
    else:
        print("No matching files found.")

if __name__ == "__main__":
    target_dir = os.path.join(os.path.abspath(os.curdir),'whatsappbot')
    os.chdir(target_dir)
    clean_own_directory()
