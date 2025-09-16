"""
Legacy run_app.py - redirects to dynamic version
This file is kept for backward compatibility
"""

import subprocess
import sys
import os

def main():
    # Redirect to the dynamic version
    dynamic_script = os.path.join(os.path.dirname(__file__), 'run_app_dynamic.py')
    
    if not os.path.exists(dynamic_script):
        print("Error: Dynamic version not found. Please use run_app_dynamic.py directly.")
        sys.exit(1)
    
    # Pass all arguments to the dynamic version
    subprocess.run([sys.executable, dynamic_script] + sys.argv[1:])

if __name__ == "__main__":
    main()
