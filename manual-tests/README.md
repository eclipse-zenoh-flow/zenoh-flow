# Automated Test 
This script was created to test automatically a specific zenoh-flow scenario. 

## How to run
To do this you need to run :
```bash
./automated-test.py
```
## what the script does
The script will check for you:
- the Zenoh-Flow configuration has been instantiated.
- the correctness of node connections 
- data from Source nodes are received by Sink nodes.
- the values received are in accordance with the data transmitted and also with the changes expected to be applied to that data.

At the end of the script this will show you good tests and failed ones.
