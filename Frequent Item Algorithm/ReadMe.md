The data (tweetstream) is available at: https://www.cs.duke.edu/courses/fall15/compsci590.4/assignment2/tweetstream.zip
Format of tweets: https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object

Algorithm for finding frequent item:

Maintain a list of items being counted. Initially the list is empty. 
For each item, if it is the same as some item on the list, increment its counter by one. 
If it differs from all the items on the list, then if there are less than k items on the list, add the item to the list with its counter set to one.
If there are already k items on the list decrement each of the current counters by one. Delete an element from the list if its count becomes zero.
