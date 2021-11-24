from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import csv
import pandas as pd

#columns = 'movie_id,movie_title,movie_release_year,movie_url,movie_title_language,movie_popularity,movie_image_url,director_id,director_name,director_url'
columns = 'movie_id,rating_id,rating_url,rating_score,rating_timestamp_utc,critic,critic_likes,critic_comments,user_id,user_trialist,user_subscriber,user_eligible_for_trial,user_has_payment_method,movie_title,movie_release_year,movie_url,movie_title_language,movie_popularity,movie_image_url,director_id,director_name,director_url'.split(',')
#columns = 'movie_id,rating_id,rating_url,rating_score,rating_timestamp_utc,critic,critic_likes,critic_comments,user_id,user_trialist,user_subscriber,user_eligible_for_trial,user_has_payment_method'
class MRMostPlateID(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_PlateID,
                   combiner=self.combiner_count_PlateID,
                   reducer=self.reducer_count_PlateID),
            MRStep(reducer=self.reducer_find_max_PlateID)
        ]

    def mapper_get_PlateID(self, _, line):
       reader = csv.reader([line])
       for row in reader:
           zipped=zip(columns,row)
           diction=dict(zipped)
           VehicleYear=diction['rating_timestamp_utc']
           #outputing as key value pairs
           yield VehicleYear, 1

    def combiner_count_PlateID(self, word, counts):
        # optimization: sum the words we've seen so far
        yield (word, sum(counts))

    def reducer_count_PlateID(self, word, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        yield None, (sum(counts), word)

    # discard the key; it is just None
    def reducer_find_max_PlateID(self, _, word_count_pairs):
        # each item of word_count_pairs is (count, word),
        # so yielding one results in key=counts, value=word
        yield max(word_count_pairs)


if __name__ == '__main__':
    MRMostPlateID.run()
#https://mrjob.readthedocs.io/en/latest/guides/quickstart.html