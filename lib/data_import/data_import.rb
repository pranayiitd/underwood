require 'twitter'
require 'awesome_print'
require 'json'
require 'mongo'
require 'yaml'
require 'logger'

include Mongo

#####################################################
# This class is reponsible for importing data from
# various data sources. Currently reading tweets
# using twitter API client
# Author: Pranay Agarwal agarwalpranaya@gmail.com
#####################################################
class DataImport
  def initialize(twitter_client, db_coll, logger)
    @twitter_client = twitter_client
    @coll = db_coll
    @logger = logger
  end

  def parse_db_json(tweet, trend_topic, poltu_code)
    tweet_hash = tweet.attrs
    
    tweet_json = { raw_tweet: tweet_hash.to_json,
                   tid: tweet_hash[:id_str],
                   text: tweet_hash[:text],
                   trend_topic: trend_topic,
                   # TODO: Placeholder. Mapping from topic to political view
                   poltu_code: poltu_code,
                   author_id: tweet_hash[:user][:id_str],
                   author_sname: tweet_hash[:user][:screen_name]
                }
  end

  # Fetch max-sized batches of tweets, for each new tweet
  # store the raw tweet but also extract most important
  # fields like uid, retcount etc.
  # populate the right fields for the collections.
  def fetch_tweets_stream(trend_topic, type, count, poltu_code)
    response = @twitter_client.search(trend_topic,
                                      result_type: type, count: count)
    # The each enumerations will automatically make call to the next_results
    # Handling the limir_error gracefully.
    response.each do |tweet|
      # puts response.attrs[:statuses].size
      # puts response.attrs[:search_metadata]
      begin
        save_tweet(tweet, trend_topic, poltu_code)
        # break
      rescue Twitter::Error::TooManyRequests => error
        @logger.info(format('Rate limit reached. Sleeping for %s secs.',
                            error.rate_limit.reset_in))
        @logger.info($!)
        sleep error.rate_limit.reset_in + 1
        retry
      end
    end
  end

  # Save the tweet in the database after converting into the compatible
  # json doc for the mongo collection
  def save_tweet(tweet, trend_topic, poltu_code)
    tweet_db_json = parse_db_json(tweet, trend_topic, poltu_code)
    begin
      @coll.insert(tweet_db_json)
    rescue Mongo::OperationFailure
      @logger.info($!)
    end
  end
end

if __FILE__ == $PROGRAM_NAME
  twitter_cred = YAML.load_file('../../config/twitter-cred.yaml')
  db_config = YAML.load_file('../../config/db-endpoints.yaml')

  CONSUMER_KEY = twitter_cred['CONSUMER_KEY']
  CONSUMER_SECRET = twitter_cred['CONSUMER_SECRET']

  config = {
    consumer_key: CONSUMER_KEY,
    consumer_secret: CONSUMER_SECRET
  }
  logger = Logger.new('../../logs/twitter-collection-'\
    "#{Process.pid}-#{Time.now.strftime('%d-%m-%Y-%H--%M')}")
  logger.level = Logger::DEBUG

  twitter_client = Twitter::REST::Client.new(config)

  db_client = MongoClient.new(db_config['host'])
  db = db_client.db(db_config['db'])
  coll = db.collection(db_config['collection'])

  data_import_test = DataImport.new(twitter_client, coll, logger)

  puts "Starting to import tweets...."
  data_import_test.fetch_tweets_stream('#pappu', 'recent', 100, '1')
end
