# AWS SQS to Postgres

This porject consists of a python script which connects to the AWS localhost to poll messages from a SQS queue and load the information into a Postgres Database.

Steps to execute this code snippet:
* Install docker
* Pull the localhost and postgres docker images from the below links
    * [Postgres](https://hub.docker.com/r/fetchdocker/data-takehome-postgres)
    * [Localhost](https://hub.docker.com/r/fetchdocker/data-takehome-localstack)
* Create a docker environment in docker compose
* Run the environment in docker compose
* Open the environment in VS Code and create a file `docker-compose.yml`
* Attach this below code in `docker-compose.yml`
```version: "3.9"
services:
  localstack:
    image: fetchdocker/data-takehome-localstack
    ports:
      - "4566:4566"
  postgres:
    image: fetchdocker/data-takehome-postgres
    ports:  
      - 5432:5432
```
* Open Terminal and execute `docker compose up -d`
* Now the Postgres and Localhost containers are up and running. 
* Install python in local system from [Python](https://www.python.org/downloads/).
* Now clone the `requirement.txt` file from the repo.
* Run `pip install -r requirements.txt` 
* Clone the `SQStoPostgres.py` script and run the command `python SQStoPostgres.py`

This will load the sqs messages into `user_logins` table in postgres. This can be verified by running the below commands in VS Code terminal:
* `docker exec -it comdockerdevenvironmentscode-postgres-1 psql -U postgres`
* `select * from user_logins;`
This will show the records inserted into the table. 


## Questions
### How would you deploy this application in production?
We can deploy this application in production using AWS Lambda function and set up trigger to SQS message generation to call the Lambda function.
We can also use YARN to schedule the the script to run in a scheduled interval.


### What other components would you want to add to make this production ready?
* Scaling: As the data increases and the speed of SQS message generation increase we would need to look at the scaling operations.
* Error Handling: As observed in this example we might some times get invalid or incorrect data sets. We can implement  error handling in such situations.
* Also implement the `UPSERT` mechanism to update already existing records and insert only new records.

### How can this application scale with a growing dataset?
As the dataset increases we can look into the concepts of scaling the SQS queue to have more queue readers, also we can use partitioning to split the data into smaller and manageable chunks so that time to  query over it reduces.

### How can PII be recovered later on?
Currently I have used hashing with sha256 and only keeping the first 8 characters. This does not help us in recovering the PII later. But if we want to recover the PII later we can store the mapping of hash function to the PII value in a seperate table. 

### What are the assumptions you made?
My assumptions in developing this process are:
* If the SQS queue does not have any messages while it is polling , I would wait for 10 seconds to see if any message flows or would terminate the program. 
* The app_version field is mentioned to be stored as a integer but the values coming are in the format of "x.x.x" which is not a whole number. For this reason I have converted the data tyoe of `app_versio` to `nvarchar`
* I have not implemented an `upsert` operation on the duplicate messages assuming all the valid messages are inserted and deleted from the queue not resulting in a duplication.

 
