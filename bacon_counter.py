# import mrjob
from mrjob.job import MRJob

# Create a class called Bacon_count, which inherits, 
# or takes properties, from the MRJob class. We create 
# this class to be called to run the full MapReduce job withMRJob:
class Bacon_count(MRJob):

    # Next, create a mapper()function that will take (self, _, line) as parameters. 
    # The mapper() function will assign the input to key-value pairs:
   def mapper(self, _, line):

        # The second parameter (here using an underscore (_), explained next) 
        # allows methods to be mapped together. Since we are not chaining anything together, 
        # we use the Python convention of an underscore to indicate that we won’t use 
        # this parameter. The line parameter will be the line of text taken from the raw input file.
        
        # The function will loop through each word in the line of text        
       for word in line.split():
           if word.lower() == "bacon":
                # When you call a function with yield it returns what is called a generator object. 
                # A generator is an iterator like a list, however unlike a list the contents 
                # are not stored in memory, useful for large files. When yield is called 
                # the function is suspended and returns a value. A generator won't 
                # return another value until next() is called, which is something 
                # that mrJobs calls a number of times till it is done. So, for a yield, 
                # each time the word "bacon" appears, mrJobs returns “bacon”, 1. If "bacon" 
                # appears three times, then an output of “bacon”, 1 would be produced three times.
               yield "bacon", 1

   def reducer(self, key, values):
       yield key, sum(values)

if __name__ == "__main__":
   Bacon_count.run()