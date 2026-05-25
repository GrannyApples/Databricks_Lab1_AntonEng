# Databricks_Lab1_AntonEng


#Setup
place it in a _setup notebook inside the explorations folder

need to run this start of each cell one time for the notebook to work, dont like having filepaths, so I did this since __file__ for the path doesnt work in databricks

import sys
PROJECT_ROOT = "/Workspace/Users/<email>/<projectname(root)/<pyproject location> if pyproject is in root this last one isnt needed"

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)