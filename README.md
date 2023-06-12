# MongoDB Fake Workload
This python script generates a fake MongoDB workload.

This script will alternate between inserts, updates, reads and deletes for each time frame.

You can change the total runtime and the size of the frames in the constants.

# How to run

Update the script to set your MongoDB URI:

```
MONGODB_CONNECTION_STRING = "mongodb://localhost"
```

Then just run the script.

# Author

Maxime Beugnet

# Usage

Use at your own risk! This can generate a lot of data very fast...

If you have a database named `_workload_`, it'll be nuked.
