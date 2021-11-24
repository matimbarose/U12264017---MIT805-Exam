from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import csv
import pandas as pd

#columns = 'SummonsNumber,PlateID,RegistrationState,PlateType,IssueDate,ViolationCode,VehicleBodyType,VehicleMake,IssuingAgency,StreetCode1,StreetCode2,StreetCode3,VehicleExpirationDate,ViolationLocation,ViolationPrecinct,IssuerPrecinct,IssuerCode,IssuerCommand,IssuerSquad,ViolationTime,TimeFirstObserved,ViolationCounty,ViolationInFrontOfOrOpposite,HouseNumber,StreetName,IntersectingStreet,DateFirstObserved,LawSection,SubDivision,ViolationLegalCode,DaysParkingInEffect,FromHoursInEffect,ToHoursInEffect,VehicleColor,UnregisteredVehicle,VehicleYear,MeterNumber,FeetFromCurb,ViolationPostCode,ViolationDescription,NoStandingOrStoppingViolation,HydrantViolation,DoubleParkingViolation'.split(',')
columns = 'movie_id,rating_id,rating_score,rating_timestamp_utc,critic,critic_likes,critic_comments,user_id,user_trialist,user_subscriber,user_eligible_for_trial,user_has_payment_method,movie_title,movie_release_year,movie_title_language,movie_popularity,director_id,director_name'.split(',')

class MRMostViewedMovie(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ViewedMovie,
                   combiner=self.combiner_count_ViewedMovie,
                   reducer=self.reducer_count_ViewedMovie),
            MRStep(reducer=self.reducer_find_max_ViewedMovie)
        ]

    def mapper_get_ViewedMovie(self, _, line):
       reader = csv.reader([line])
       for row in reader:
           zipped=zip(columns,row)
           diction=dict(zipped)
           movie_title=diction['movie_id']
           #outputing as key value pairs
           yield movie_title, 1

    def combiner_count_ViewedMovie(self, word, counts):
        # optimization: sum the words we've seen so far
        yield (word, sum(counts))

    def reducer_count_ViewedMovie(self, word, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        yield None, (sum(counts), word)

    # discard the key; it is just None
    def reducer_find_max_ViewedMovie(self, _, word_count_pairs):
        # each item of word_count_pairs is (count, word),
        # so yielding one results in key=counts, value=word
        yield max(word_count_pairs)


if __name__ == '__main__':
    MRMostViewedMovie.run()
#https://mrjob.readthedocs.io/en/latest/guides/quickstart.html