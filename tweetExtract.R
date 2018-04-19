#Libraries
library(twitteR)
library(RMySQL)

#Authentication credentials for twitter API
#You get the credentials from your twitter account
api_key = 'your_key'
api_secret = 'your_secret'
access_token = 'token'
access_token_secret = 'token_secret'

#Twitter accounts for extraction
accounts = c("WSJ", "barronsonline", "BW", "business", "markets", "Bloomberg", "BloombergTV", "bpolitics", 
             "EconBizFin", "ReutersBiz", "FinancialTimes", "telebusiness", "BBCBusiness", "BusinessDesk", 
             "SkyNewsBiz", "tijd", "handelsblatt", "sole24ore", "LesEchos", "nikkei", "NAR", "BusinessDay", 
             "FinancialReview", "aus_business", "financialpost", "BNN", "CBCBusiness", "globebusiness", 
             "SBH_USA", "ChinaBizWatch")

#Connect to MySQL
myDB = dbConnect(MySQL(), user = 'username', password = 'pass', dbname = 'databaseName', host = '127.0.0.1')

#Twitter authentication
setup_twitter_oauth(api_key, api_secret, access_token, access_token_secret)

#Extract tweets from each account
for (accountIndex in 1:length(accounts))
{
  #User timeline of accounts
  tweets = userTimeline(accounts[accountIndex], n = 3200)
  tweets.df = twListToDF(tweets)
  tweets.df$text = gsub('"', '', tweets.df$text)
  
  #Create query
  query = sprintf("SELECT created, id FROM %s WHERE created = (SELECT MAX(created) FROM %s)", accounts[accountIndex], accounts[accountIndex])
  result = dbSendQuery(myDB, query)
  row = fetch(result, n = 1)
  time = strptime(row[1], "%Y-%m-%d %H:%M:%S", tz = "UTC")
  dbClearResult(result)
  count = 0;
  
  #Insert each tweet
  for (tweetIndex in 1:nrow(tweets.df))
  {
    if (tweets.df[tweetIndex, 5] > time & tweets.df[tweetIndex, 8] != row[2])
    {
      query = sprintf("INSERT INTO %s (text, favouriteCount, created, id, screenName, retweetCount) VALUES(\"%s\", %i, \"%s\", \"%s\", \"%s\", %i)", accounts[accountIndex], tweets.df[tweetIndex, 1], tweets.df[tweetIndex, 3], tweets.df[tweetIndex, 5], tweets.df[tweetIndex, 8], tweets.df[tweetIndex, 11], tweets.df[tweetIndex, 12])
      clearQuery = dbSendQuery(myDB, query)
      dbClearResult(clearQuery)
      count = count + 1
    }
  }
  print(sprintf("%s updated with %i new tweets.", accounts[accountIndex], count))
}

dbDisconnect(myDB)
