# dd-csv-parser-go

Spike get figures for performance of golang for pre parsing a V3 csv file to extract distinct dimensions.

### Running
Update `run.sh` set `INPUT_FILE=` to the path of the file you want to pass in.
Update file permissions of `run.sh` to make executable and then `./run.sh`

### Results
Results of parsing file (time taken, rows process and number of dimensions found) will be written to `results.txt` in the project root dir.
