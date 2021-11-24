from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import csv
import pandas as pd

#columns = 'SummonsNumber,PlateID,RegistrationState,PlateType,IssueDate,ViolationCode,VehicleBodyType,VehicleMake,IssuingAgency,StreetCode1,StreetCode2,StreetCode3,VehicleExpirationDate,ViolationLocation,ViolationPrecinct,IssuerPrecinct,IssuerCode,IssuerCommand,IssuerSquad,ViolationTime,TimeFirstObserved,ViolationCounty,ViolationInFrontOfOrOpposite,HouseNumber,StreetName,IntersectingStreet,DateFirstObserved,LawSection,SubDivision,ViolationLegalCode,DaysParkingInEffect,FromHoursInEffect,ToHoursInEffect,VehicleColor,UnregisteredVehicle,VehicleYear,MeterNumber,FeetFromCurb,ViolationPostCode,ViolationDescription,NoStandingOrStoppingViolation,HydrantViolation,DoubleParkingViolation'.split(',')
columns = 'movie_id,rating_id,rating_score,rating_timestamp_utc,critic,critic_likes,critic_comments,user_id,user_trialist,user_subscriber,user_eligible_for_trial,user_has_payment_method,movie_title,movie_release_year,movie_title_language,movie_popularity,director_id,director_name'.split(',')

class MRMostMostDirector(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_MostDirector,
                   combiner=self.combiner_count_MostDirector,
                   reducer=self.reducer_count_MostDirector),
            MRStep(reducer=self.reducer_find_max_MostDirector)
        ]

    def mapper_get_MostDirector(self, _, line):
       reader = csv.reader([line])
       for row in reader:
           zipped=zip(columns,row)
           diction=dict(zipped)
           director_name=diction['director_name']
           #outputing as key value pairs
           yield director_name, 1

    def combiner_count_MostDirector(self, word, counts):
        # optimization: sum the words we've seen so far
        yield (word, sum(counts))

    def reducer_count_MostDirector(self, word, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        yield None, (sum(counts), word)

    # discard the key; it is just None
    def reducer_find_max_MostDirector(self, _, word_count_pairs):
        # each item of word_count_pairs is (count, word),
        # so yielding one results in key=counts, value=word
        yield max(word_count_pairs)


if __name__ == '__main__':
    MRMostMostDirector.run()
#https://mrjob.readthedocs.io/en/latest/guides/quickstart.html