import pymysql
import boto3
import re
import json
import random
import time
import base64
from botocore.exceptions import ClientError
from botocore.config import Config

# variables to determine which tweets to pull
retrieve_time = 3
score_threshold = 20

# array of unimportant words to filter out
unimportant_words = ["a", "about", "above", "above", "across", "after", "afterwards", "again", "against", "all",
                     "almost", "alone", "along", "already", "also", "although", "always", "am",
                     "among", "amongst", "amoungst", "amount", "an", "and", "another", "any", "anyhow", "anyone",
                     "anything", "anyway", "anywhere", "are", "around", "as", "at", "back", "be", "became", "because",
                     "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below",
                     "beside", "besides", "between", "beyond", "bill", "both", "bottom", "but", "by", "call", "can",
                     "cannot", "cant", "co", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done",
                     "down", "due", "during", "each", "eg", "eight", "either", "eleven", "else", "elsewhere",
                     "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere",
                     "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former",
                     "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go",
                     "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby",
                     "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred",
                     "ie", "if", "in", "inc", "I'm", "im", "i've", "ive", "indeed", "interest", "into", "is", "it",
                     "its", "itself", "keep",
                     "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile",
                     "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must",
                     "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no",
                     "nobody", "none", "noone", "nor", "nothing", "now", "nowhere", "of", "off", "often",
                     "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours",
                     "ourselves", "out", "over", "own", "part", "per", "perhaps", "please", "put", "rather", "re",
                     "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show",
                     "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone",
                     "something", "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten",
                     "than", "that", "the", "their", "them", "themselves", "then", "thence", "there",
                     "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thick", "thin",
                     "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus",
                     "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under",
                     "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what",
                     "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein",
                     "whereupon", "wherever", "whether", "which", "while", "whither",
                     "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within", "without", "would",
                     "yet", "you", "your", "yours", "yourself", "yourselves", "the", "u", "thing"]

# email information
sender = "Project Moodswing <projectmoodswing@firstdata.com>"
aws_region = "us-east-1"
body_text = ("Issue with Product")
charset = "UTF-8"


# get_secret function
#   gets secret database credentials
#   returns information as a string
def get_secret():
    secret_name = "ProjMoodswing/MySQL"
    session = boto3.session.Session()
    # creates new client to retrieve secret
    client = session.client(service_name='secretsmanager',
                            region_name=aws_region,
                            config=Config(proxies={'https': 'http://fdcproxy.1dc.com:8080'}))
    # gets the secret as a string
    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
    if 'SecretString' in get_secret_value_response:
        return get_secret_value_response['SecretString']
    else:
        return base64.b64decode(get_secret_value_response['SecretBinary'])


# get_triggers function
#   takes connection as parameter
#   queries the database
#   finds the products that fall into the threshold score
#   returns a list of product groups that fall above threshold
def get_triggers(connection):
    triggers = []
    with connection.cursor() as cur:
        sql = """create view alert as select product_group, neg_sent, mixed_sent, sum((favorite_count * .2) + (retweet_count * .8) + (neg_sent * 10)) as score
        from tweets where created_at > date_sub(now(), interval %s hour) and neg_sent >=.85 and mixed_sent < 0.03
        group by product_group"""
        cur.execute(sql, (retrieve_time))
        connection.commit()
        sql = """select product_group from alert where score >= %s;"""
        cur.execute(sql, (score_threshold))
        connection.commit()
        cur.close()
        for row in cur:
            triggers.append(list(row)[0])
    with connection.cursor() as cur:
        sql = """DROP VIEW alert;"""
        cur.execute(sql)
        connection.commit()
    return triggers


# get_email_info function
#   takes one trigger and connection as parameters
#   queries the database
#   builds the html for the email
#   returns the subject of the email, the html, and the recipients
def get_email_info(trigger, connection):
    flagged_tweet = []
    tweet_text = ""
    num_tweets = []
    recipients = []
    tweet_links = []
    # sql query that gets the 'worst' tweet i.e. with the highest negativity score
    with connection.cursor() as cur:
        sql = """create view top_tweet as select id_str, tweet_text, retweet_count, favorite_count, 
        created_at, product_group, ((favorite_count * .2) + (retweet_count * .8) + (neg_sent * 10)) as score
        from tweets"""
        cur.execute(sql)
        connection.commit()
        sql = """select tweet_text from top_tweet where created_at > date_sub(now(), interval %s hour) 
        and product_group = %s order by score desc limit 1"""
        cur.execute(sql, (retrieve_time, trigger))
        connection.commit()
        cur.close()
        for row in cur:
            flagged_tweet.append(list(row)[0])
    with connection.cursor() as cur:
        sql = """DROP VIEW top_tweet;"""
        cur.execute(sql)
        connection.commit()

    # sql query that gets the number of mentions for the product_group
    with connection.cursor() as cur:
        sql = """select (sum(retweet_count) + count(id_str)) as num_tweets from tweets
            where product_group = %s and created_at > date_sub(now(), interval %s hour);"""
        cur.execute(sql, (trigger, retrieve_time))
        connection.commit()
        cur.close()
        for row in cur:
            num_tweets.append(list(row)[0])

    # sql query that gets the recipients of the email from the database
    with connection.cursor() as cur:
        sql = """ select c_email from contacts where product_group = %s; """
        cur.execute(sql, (trigger))
        connection.commit()
        cur.close()
        for row in cur:
            recipients.append(list(row)[0])

    # sql query that gets sample text from each negative tweet
    with connection.cursor() as cur:
        sql = """select tweet_text from tweets where created_at > date_sub(now(), interval %s hour) and product_group= %s and 
        sentiment = 'NEGATIVE'"""
        cur.execute(sql, (retrieve_time, trigger))
        connection.commit()
        cur.close()
        for row in cur:
            tweet_text = tweet_text + " " + (list(row)[0])

    # sql query that gets the third most negative tweets to display as links in the email body
    with connection.cursor() as cur:
        sql = """create view link as select id_str, tweet_text, retweet_count, favorite_count, 
        created_at, product_group, ((favorite_count * .2) + (retweet_count * .8) + (neg_sent * 10)) as score
        from tweets"""
        cur.execute(sql)
        connection.commit()
        sql = """select id_str from link where created_at > date_sub(now(), interval %s hour) 
        and product_group = %s order by score desc limit 3"""
        cur.execute(sql, (retrieve_time, trigger))
        connection.commit()
        cur.close()
        for row in cur:
            tweet_links.append(list(row)[0])
    with connection.cursor() as cur:
        sql = """DROP VIEW link;"""
        cur.execute(sql)
        connection.commit()

        # splits up text of the tweets
    words_in_tweets = re.findall(r'\w+', tweet_text)
    mentioned_words = []

    # loops through each word and checks that it isn't repeated and that it is an important word
    # and adds it to the array mentioned_words
    for word in words_in_tweets:
        if word.lower() not in mentioned_words:
            if word.lower() not in unimportant_words:
                mentioned_words.append(word.lower())

    # selects 5 random words to display in the email body
    words_for_email = ""
    for x in range(5):
        random_index = random.randint(0, len(mentioned_words) - 1)
        words_for_email = words_for_email + ", " + (mentioned_words[random_index])
        mentioned_words.remove(mentioned_words[random_index])

    # creates the subject for the email
    subject = "Issue with " + trigger.capitalize()

    # creates the email html using the information from the sql queries
    body_html = """<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
	<head>
		<meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
		<meta name="format-detection" content="telephone=no"> 
		<meta name="viewport" content="width=device-width; initial-scale=1.0; maximum-scale=1.0; user-scalable=no;">
		<meta http-equiv="X-UA-Compatible" content="IE=9; IE=8; IE=7; IE=EDGE" />
		<title>Project MoodSwing Email Alert1</title>
		<style type="text/css"> 
        	@media screen and (max-width: 630px) {
        	}
         @import url(http://fonts.googleapis.com/css?family=Roboto:300); /*Calling our web font*/

        /* Some resets and issue fixes */
        #outlook a { padding:0; }
        body{ width:100% !important; -webkit-text; size-adjust:100%; -ms-text-size-adjust:100%; margin:0; padding:0; }     
        .ReadMsgBody { width: 100%; }
        .ExternalClass {width:100%;} 
        .backgroundTable {margin:0 auto; padding:0; width:100%;!important;} 
        table td {border-collapse: collapse;}
        .ExternalClass * {line-height: 115%;}       
        /* End reset */


        /* These are our tablet/medium screen media queries */
        @media screen and (max-width: 630px){


            /* Display block allows us to stack elements */                      
            *[class="mobile-column"] {display: block;} 

            /* Some more stacking elements */
            *[class="mob-column"] {float: none !important;width: 100% !important;}     

            /* Hide stuff */
            *[class="hide"] {display:none !important;}          

            /* This sets elements to 100% width and fixes the height issues too, a god send */
            *[class="100p"] {width:100% !important; height:auto !important;}                    

            /* For the 2x2 stack */         
            *[class="condensed"] {padding-bottom:40px !important; display: block;}

            /* Centers content on mobile */
            *[class="center"] {text-align:center !important; width:100% !important; height:auto !important;}            

            /* 100percent width section with 20px padding */
            *[class="100pad"] {width:100% !important; padding:20px;} 

            /* 100percent width section with 20px padding left & right */
            *[class="100padleftright"] {width:100% !important; padding:0 20px 0 20px;} 

            /* 100percent width section with 20px padding top & bottom */
            *[class="100padtopbottom"] {width:100% !important; padding:20px 0px 20px 0px;} 
       	 }

   		</style>
	</head>

 <table width="640" border="0" cellspacing="0" cellpadding="18" bgcolor="#365366" class="100p" align="center">
    <tr>
        <td align="center" style="color:#FFFFFF; font-size: 44px;">
                                                            <font face="'Harlow Solid Italic', Arial, sans-serif">
                                                                <span style="font-size:44px;">Project MoodSwing
                                                                </span>
                                                           	<br />
                                                        		<span align="center" style="color:#FFFFFF; font-size: 24;">
                                                        		</span>
                                                        	<font face= "'Bahnschrift Light SemiCondensed', Arial, sans-serif">
                                                        		<SPAN style="font-size: 24px;">
                                                        		Twitter Alert
                                                        		</SPAN>
                                                        	</font>	
                                                        	<br />
                                                        	<font face= "'Bahnschrift Light SemiCondensed', Arial, sans-serif">
                                                        		<SPAN style="font-size: 24px;">
                                                        		FISERV
                                                        		</SPAN>
                                                        	</font>	
                                                        </td>

    </tr>
</table>
<!-- below is the what happened section-->

 <table width="640" border="0" cellspacing="0" cellpadding="18" bgcolor="#52aaea" class="100p" align="center">
    <tr>
        <td align="center" style="font-size:18px; color:#FFFFFF;"><font face="'Roboto', Arial, sans-serif">What Happened?</font></td>
    </tr>
</table>

<!-- The mention and flagged product section-->

<table width="640" border="0" cellspacing="0" cellpadding="20" class="100p" bgcolor="#FFFFFF" align="center" >
    <tr>
        <td align="center" valign="top">
            <table border="0" cellpadding="0" cellspacing="0" class="100padtopbottom" width="600">
                <tr>
                                <td align="center" class="condensed" valign="top">
                                    <table align="center" border="0" cellpadding="0" cellspacing="0" class="mob-column" width="290">
                                        <tr>
                                            <td valign="top" align="center">
                                                <table border="0" cellspacing="0" cellpadding="2">
                                                    <tr>
                                                        <td valign="top" align="center" class="100padleftright">
                                                            <table border="0" cellspacing="0" cellpadding="0">
                                                                <tr>
                                                                	<td valign="center" width="135" align="center" style="font-size:16px; color:#365366;"><font face="'Roboto', Arial, sans-serif">Number of mentions:</font></td>
                                                                    <td width="20"></td>
                                                                    <td valign="top" width="135" align="center"  style="font-size:16px; color:#365366;"><font face="'Roboto', Arial, sans-serif">The flagged product: </font></td>
                                                                </tr>
                                                            </table>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <td height="10"></td>
                                                    </tr>
                                                    <tr>
                                                        <td valign="center" align="center" class="100padleftright">
                                                            <table border="0" cellspacing="0" cellpadding="0">
                                                                <tr>
                                                                	<td valign="center" width="135" align="center" style="font-size:25px; color:#365366;"><font face="'Roboto', Arial, sans-serif">""" + str(
        num_tweets[0]) + """</font></td>
                                                                    <td width="20"></td>
                                                                    <td valign="center" width="135" align="center"  style="font-size:25px; color:#365366;"><font face="'Roboto', Arial, sans-serif">""" + trigger + """</font></td>
                                                                </tr>
                                                            </table>
                                                        </td>
                                                    </tr>
                                                </table>
                                            </td>
                                        </tr>
                                    </table>
                                </td>
                </tr>
            </table>
        </td>
    </tr>
</table>
<!-- The bottom section-->
<table width="640" border="0" cellspacing="0" cellpadding="20" bgcolor="#52aaea" class="100p" align="center">
    <tr>
    	<td align="center" style="font-size:16px; color:#365366;">
       	<font face="'Roboto', Arial, sans-serif">
       	<span style="color:#365366; font-size:24px;">Mentioned Words</span>
            </font>
        </td>
    </tr>
</table>
<table width="640" border="0" cellspacing="0" cellpadding="20" bgcolor="#ffffff" class="100p" align="center">
                <tr>
                    <td align="center" style="font-size:16px; color:#2a8e9d;"><font face="'Roboto', Arial, sans-serif"><span style="color:#2a8e9d; font-size:16px;">""" + words_for_email[
                                                                                                                                                                          2:len(
                                                                                                                                                                              words_for_email)] + """</span>
                       </font>
                    </td>
                </tr>
            </table>
<table width="640" border="0" cellspacing="0" cellpadding="20" bgcolor="#52aaea" class="100p" align="center">
    <tr>
    	<td align="center" style="font-size:16px; color:#365366;">
       	<font face="'Roboto', Arial, sans-serif">
       	<span style="color:#365366; font-size:24px;">Example Tweet</span>
            </font>
        </td>
    </tr>
</table>
<table width="640" border="0" cellspacing="0" cellpadding="20" bgcolor="#ffffff" class="100p" align="center">
                <tr>
                    <td align="center" style="font-size:16px; color:#2a8e9d;"><font face="'Roboto', Arial, sans-serif"><span style="color:#2a8e9d; font-size:16px;">""" + \
                flagged_tweet[0] + """</span>
                       </font>
                    </td>
                </tr>
            </table>
<table width="640" border="0" cellspacing="0" cellpadding="20" bgcolor="#52aaea" class="100p" align="center">
    <tr>
    	<td align="center" style="font-size:16px; color:#365366;">
       	<font face="'Roboto', Arial, sans-serif">
       	<span style="color:#365366; font-size:24px;">Links to Tweets</span>
            </font>
        </td>
    </tr>
</table>
<table width="640" border="0" cellspacing="0" cellpadding="20" bgcolor="#ffffff" class="100p" align="center">
                <tr>
                    <td align="center" style="font-size:16px; color:#2a8e9d;"><font face="'Roboto', Arial, sans-serif"><span style="color:#2a8e9d; font-size:16px;"><a href="https://twitter.com/pmoodswing/status/""" + \
                tweet_links[0] + """">Link to first tweet</a></span>
                       </font>
                    </td>
                </tr>
            </table>
<table width="640" border="0" cellspacing="0" cellpadding="20" bgcolor="#ffffff" class="100p" align="center">
                <tr>
                    <td align="center" style="font-size:16px; color:#2a8e9d;"><font face="'Roboto', Arial, sans-serif"><span style="color:#2a8e9d; font-size:16px;"><a href="https://twitter.com/pmoodswing/status/""" + \
                tweet_links[1] + """">Link to second tweet</a></span>
                       </font>
                    </td>
                </tr>
            </table>
<table width="640" border="0" cellspacing="0" cellpadding="20" bgcolor="#ffffff" class="100p" align="center">
                <tr>
                    <td align="center" style="font-size:16px; color:#2a8e9d;"><font face="'Roboto', Arial, sans-serif"><span style="color:#2a8e9d; font-size:16px;"><a href="https://twitter.com/pmoodswing/status/""" + \
                tweet_links[2] + """">Link to third tweet</a></span>
                       </font>
                    </td>
                </tr>
            </table>
<!-- below is the bottom section-->
<table width="640" border="0" cellspacing="0" cellpadding="20" bgcolor="#365366" class="100p" align="center">
                <tr>
                    <td align="center" style="font-size:16px; color:#ffffff;"><font face="'Roboto', Arial, sans-serif"><span style="color:#52aaea; font-size:24px;">What is Project MoodSwing?</span><br />
                        <br />
                        <span style="font-size:16px;">Project MoodSwing is a program that searches through Twitter for mentions about First Data, Fiserv, and our products like Clover, Telecheck, and Star. We use Amazon comprehend to pull the negative tweets. When there is a spike in the average number of mentions within an hour, we send an automated email to a team member who works with the specific product.</span></font>
                    </td>
                </tr>
            </table>
            <table width="640" border="0" cellspacing="0" cellpadding="20" bgcolor="#ffffff" class="100p" align="center">
               <tr>
                   <td align="center" style="font-size:16px; color:#2a8e9d;"><font face="'Roboto', Arial, sans-serif"><span style="color:#2a8e9d; font-size:16px;">If you would like to opt-out, please contact projectmoodswing@firstdata.com</a></span>
                      </font>
                   </td>
               </tr>
           </table>
            """
    # returns information for the email
    return {"Subject": "Issue with " + trigger.capitalize(), "BodyHtml": body_html, "Recipient": recipients}


def lambda_handler(event, context):
    # gets the secret information to log in to the database
    secret = get_secret()
    secret_dict = eval(secret)
    # connects to the database
    conn = pymysql.connect(host=secret_dict['host'],
                           user=secret_dict['username'],
                           password=secret_dict['password'],
                           database=secret_dict['dbname'],
                           connect_timeout=10)
    # gets the triggers that are above the threshold
    triggers = get_triggers(conn)

    # loops through each triggered product group
    for trigger in triggers:
        # gets the email information for the email
        payload = get_email_info(trigger, conn)

        # creates the client for the simple email service, including the proxy to get around the First Data firewall
        ses = boto3.client('ses', region_name=aws_region,
                           config=Config(proxies={'https': 'http://fdcproxy.1dc.com:8080'}))

        # gets the variables for the email
        recipients = payload['Recipient']
        email_subject = payload['Subject']
        body_html = payload['BodyHtml']

        # loops through each recipient for the product
        for recipient in recipients:
            # try-catch that sends the email
            try:
                response = ses.send_email(
                    Source=sender,
                    Destination={
                        'ToAddresses': [
                            recipient,
                        ],
                    },
                    Message={
                        'Body': {
                            "Html": {
                                'Charset': charset,
                                'Data': body_html,
                            },
                            'Text': {
                                'Charset': charset,
                                'Data': body_text,
                            },
                        },
                        'Subject': {
                            'Charset': charset,
                            'Data': email_subject,
                        },
                    },
                )
            except ClientError as e:
                print(e.response['Error']['Message'])
            else:
                print("Email sent! Message ID: "),
                print(response['MessageId'])

    # close the database connection
    conn.close()
