# Automated Test 
## What the script does
In this folder there is a python script to automatically test the launch of a zenoh-flow configuration.

At first the script starts to find the zenoh and zenoh-flow executables and the configuration file paths, this is necessary to build the command to be able to run Zenoh with Zenoh-Flow as plugin.
Furthermore the test makes sure that the zenoh configuration is able to run zenoh-flow as a plugin.

## How does the test work?
This test use the getting-started example to validate the zenoh-flow functionality. 

The script test verifies:
- Zenoh with zenoh-flow plugin is up and running.
- The example runs correctly.
- That the number of active Zenoh-Flow nodes is equal to the number of nodes declared into the yaml file.

## How to run
To do this you need to run :
```bash
pip install psutil
pip install PyYAML
python3 automated-test.py  
```
