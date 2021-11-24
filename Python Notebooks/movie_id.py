from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
import pandas as pd
#split by ,
columns = 'movie_id,rating_id,rating_url,rating_score,rating_timestamp_utc,critic,critic_likes,critic_comments,user_id,user_trialist,user_subscriber,user_eligible_for_trial,user_has_payment_method'
class movie_id(MRJob):
    def steps(self):
        return[
            MRStep(mapper=self.mapper_get_movie_id,
                  reducer=self.reducer_count_movie_id)
        ]
#Mapper function 
    def mapper_get_movie_id(self, _, line):
       reader = csv.reader([line])
       for row in reader:
           zipped=zip(columns,row)
           diction=dict(zipped)
           movie_id=diction['movie_id']
           #Fine_amount=diction['ViolationDescription']
           if movie_id=='movie_id':
                movie_id=0
           #outputing as key value pairs
           yield movie_id, 1
           #yield ratings, 1
           #yield ratings, pd.to_numeric(Fine_amount)
#Reducer function
    def reducer_count_movie_id(self, key, values):
       yield   key, sum(values)
if __name__ == "__main__":
    movie_id.run()