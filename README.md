# Combo2Mongo
Process a folders with combos and store them in **MongoDB**.

## Basic How-To
Just modify the ```combo2mongo_cfg.py``` to use your own configuration.

To run, just launch ```combo2mongo.py```.

## What's **Mongo2Combo** purpouse?
It takes any ```combo file```, search for an email, and store it as the key of a MongoDB document. The other fields are stored too, as the value.
If no mail is found, then the first field will be stored as the key of the document.

## Config options
*  ```rootPath``` ⇒ Main folder where the combos are located (it will search inside nested folders).
*  ```checkpointFile``` ⇒ **Combo2Mongo** will store a checkpoint of already processed files, so if it fails, it can resume after the last file.
*  ```batchSize``` ⇒ **Combo2Mongo** work with bulk writes against **MongoDB**, to achieve a good performance. The number of operations that will travel with each batch can be changed here
*  ```storeSource``` ⇒ If it's set to **True**, **Combo2Mongo** will store also the full path of the source files used to get this information.
*  ```mongoConfig``` ⇒ **MongoDB** server configuration. At this very moment we are working with a local instance with no security (not hard to change, but keep that in mind!).

## WARNING
Only text files must be inside the folders, please remove any binary file.
Separator for the combo file is meant to be ```:```
