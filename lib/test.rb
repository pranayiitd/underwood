require 'twitter'
require 'awesome_print'
require 'json'
require 'mongo'
include Mongo

YOUR_CONSUMER_KEY='jtfzSbxfOaX7yhv7Gtgbo3FJ8'
YOUR_CONSUMER_SECRET='c4x8efW9c5qQi5r97ZLBzAYRWZ2XsLfu2pCCCYnARfaHM6GCOL'
config = {
    consumer_key:    YOUR_CONSUMER_KEY,
      consumer_secret: YOUR_CONSUMER_SECRET,
}


#client = Twitter::REST::Client.new(config)
#response = client.search("#feku", result_type: "recent", count: "1")
#ap response.to_h

mongo_client = MongoClient.new("localhost")
db = mongo_client.db("twitter_project")
coll = db.collection("tweets")

t = {"tid" => "1", "text" => "hello"}

begin
  coll.insert(t)
rescue Mongo::OperationFailure => ex
  #puts ex.message
  puts ex.message
end


