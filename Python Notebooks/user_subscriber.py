from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
import pandas as pd
#split by ,
columns = 'col,movie_id,rating_id,rating_score,rating_timestamp_utc,critic,critic_likes,critic_comments,user_id,user_trialist,user_subscriber,user_eligible_for_trial,user_has_payment_method,movie_title,movie_release_year,movie_title_language,movie_popularity,director_id,director_name'.split(',')
class user_subscriber(MRJob):
    def steps(self):
        return[
            MRStep(mapper=self.mapper_get_user_subscriber,
                  reducer=self.reducer_count_user_subscriber)
        ]
#Mapper function 
    def mapper_get_user_subscriber(self, _, line):
       reader = csv.reader([line])
       for row in reader:
           zipped=zip(columns,row)
           diction=dict(zipped)
           user_subscriber=diction['user_subscriber']
           #outputing as key value pairs
           yield user_subscriber, 1
           #yield user_subscriber, 1
           #yield user_subscriber, pd.to_numeric(Fine_amount)
#Reducer function
    def reducer_count_user_subscriber(self, key, values):
       yield key, sum(values)
if __name__ == "__main__":
    user_subscriber.run()