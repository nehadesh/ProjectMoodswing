# Project Mood Swing
Project Mood Swing is a tool to send alerts when an influx of negative tweets have been decected through the built in sentiment analysis. 

 

## Langauges
 * Python
 * HTML
 * CSS
 * SQL

## Installation
 We created layers in AWS so that allows us to zip files we needed to run the lambda function without having to put the imports in the code (ZIP.md)

### Services
 * Amazon Web Services
 
 	* S3 -Amazon Simple Storage Storage, provided the storage where we housed the search strings and the returned tweets, S3 allowed us to created different folders for the content being stored. We used this to help trigger the Lambda functions
 	
 	* Lambda- Allowed us to run our code without having to provision and manage servers. It self-scaled based on the computing needs and allowed us to run multiple functions at once. When the lambda function was triggered it would be able to search twitter using the many search strings we created based on the different products we wanted to see tweets for. Lmambda alao offers a console where you can edit code however *we do not reccomend it-see challenges*
 	
 	* EC2- Elastic Compute Cloud, provided resizeable computing capacity or servers, all in Amazon's datacenters. We used this to created the layers need in order to zip python packages because Amazon would not allow us to import certaint libraries without going through their system.
 	* SQS - Amazon Simple Queue Service, is a fully managed message queuing service that makes it easy to decouple and scale microservices, distributed systems, and serverless applications. We used this to push the search queries to be batched as well as the sentiment process. 
 	* CloudWatch - provided a reliable, scalable, and flexible monitoring solution that you can start using within minutes. This created the trigger to run our lambda function when files were created in s3. This is what triggered the start of our function and helped pushed th eprocess along within the code itself.
 	* RDS - Amazon Relational Database Service, is a web service that makes it easier to set up, operate, and scale a relational database in the cloud. Allowed us to store the tweets their score and other information we wanted associated with each tweet.
 		* MySQL - This was the language we used to query and append the database.
 	* Comprehend - Amazon text analysis service that allowed for us to detect how negative, positive or neutral a text was. We imported it using the boto3 client
 	  ```bash
 	  boto3.client('Comprehend')
 	  ```
   * Tweepy - A pyton library for the twitter api. This requires api keys which will be associated with the twitter account you're using to track the tweet searches. This has to be zipped in the AWS layer
   * 
     ```bash
     import tweepy
     ```

## Common Errors
 This is a lost of all the common errors we ran into during the process. Some of them are everyday errors whilst some get nore specific. SOlutions have been provided for some of them.
 * Search strings couldn’t be long – the longer they were the less results would show up, we had to whittle down our 500 character search strings to under 50 characters
 * Rate limit errors were annoying while testing – suggestion: have multiple twitter accounts so that you can switch API keys when you timeout

 
## Challenges 
 * We ran into firewalls since we were running this project inside of a private network so it was hard to be able to search twitter and pull that data into our AWS enviroment. 
 * Aws often timed out and functions did not save properly/ Lambda did not load the correct version so an older version came up verses last version modified.
 *  AWS documen
 * When searching with the search strings twitter had a 500 charcater limit that limited results
 * When using AWS there was a steep learning curve within the system itself and we had to do a lot of research which took a lot out ofthe time we had 

 * Tweet formatting
    * Encoding errors resulted when we didn’t filter out other language tweets
    * *Tip: add a language filter in the tweepy search method lang=’en’*
    * Encode the tweet text into utf-8 before writing to a file/ anywhere else, because it can cause issues


## Things To Know
 1) Tweepy syntax excludes can only be one word and should NOT be OR-ed together
    ```bash
    'clover terminal' -leaf -four -field
    ```
 2) Don't write code in AWS lambda instead save it in a text editor of some sort and then test in the console.
 3) Don't forget to set permissions for each service you use in AWS. Everything needs a permission if you're connecting it to another service in aws. 
 4) Single include words should not be within quotes
 5) Phrases should be in quotes 
    * OR the include words if you want to check for any of them in the tweets 
Ex: clover OR gyft OR ‘first data’
    * AND the include words if you want to make sure both words exist in the tweet
Ex: clover AND terminal
* Lambda layer creation must be done through an Amazon Machine Instance (AMI); you basically need to compile the python modules in a format that aws can recognize

## Future of This Project
* **Topic modelling of the tweet texts to see who needs to be notified**
    * Ex: some neutral tweets ask for some sort of email address or contact information, so we can tag these tweets with an info tag
* Modifying for other enterprise usage.
    * Mass twitter searches for specific things


## Contacts
- For any questions, comments, or concerns please contact Projectmoodswing@FirstData.com


# Contributors 
Harrison Banh
Neha Deshpande
Abbey Fagen
Anna Moody
Mitchell Peters
Aaliyah Smith
Meghan Walther
Jordan Whitaker
Kimberly Yu








