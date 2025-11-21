"""
Part 3: Measuring Performance

Now that you have drawn the dataflow graph in part 2,
this part will explore how performance of real pipelines
can differ from the theoretical model of dataflow graphs.

We will measure the performance of your pipeline
using your ThroughputHelper and LatencyHelper from HW1.

=== Coding part 1: making the input size and partitioning configurable ===

We would like to measure the throughput and latency of your PART1_PIPELINE,
but first, we need to make it configurable in:
(i) the input size
(ii) the level of parallelism.

Currently, your pipeline should have two inputs, load_input() and load_input_bigger().
You will need to change part1 by making the following additions:

- Make load_input and load_input_bigger take arguments that can be None, like this:

    def load_input(N=None, P=None)

    def load_input_bigger(N=None, P=None)

You will also need to do the same thing to q8_a and q8_b:

    def q8_a(N=None, P=None)

    def q8_b(N=None, P=None)

Here, the argument N = None is an optional parameter that, if specified, gives the size of the input
to be considered, and P = None is an optional parameter that, if specifed, gives the level of parallelism
(number of partitions) in the RDD.

You will need to make both functions work with the new signatures.
Be careful to check that the above changes should preserve the existing functionality of part1
(so python3 part1.py should still give the same output as before!)

Don't make any other changes to the function sigatures.

Once this is done, define a *new* version of the PART_1_PIPELINE, below,
that takes as input the parameters N and P.
(This time, you don't have to consider the None case.)
You should not modify the existing PART_1_PIPELINE.

You may either delete the parts of the code that save the output file, or change these to a different output file like part1-answers-temp.txt.
"""
import pyspark
import time
import os
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataflowGraphExample").getOrCreate()
sc = spark.sparkContext

def load_input(N=None, P=None):
    if isinstance(N, list):
        N = N[0]
    if N is None:
        N = 1000001
    if P is None:
        return sc.parallelize(range(1, N))
    else:
        return sc.parallelize(range(1, N), P)
    
def load_input_bigger(N=None, P=None):
    # Return a parallelized RDD with the integers between 1 and 100,000,000
    if N is None:
        N = 100000001
    
    if P is None:
        return sc.parallelize(range(1, N))
    else:
        # Use specified number of partitions
        return sc.parallelize(range(1, N), P)
    
def q8_a(N=None, P=None):
    rdd = load_input_bigger(N, P)
    return q6(rdd)

def q8_b(N=None, P=None):
    rdd = load_input_bigger(N, P)
    return q7(rdd)

def q6(rdd):
    # Input: the RDD from Q4
    if rdd.isEmpty():
        return (None, 0, None, 0)

    # Convert to (key, value) pairs so general_map works
    rdd_pairs = rdd.map(lambda x: (x, x))

    # Map all numbers into single digits
    digit_counts = general_map(rdd_pairs, lambda _, v: [(d, 1) for d in str(v)])

    # Take totals for each digit
    digit_totals = general_reduce(digit_counts, lambda a, b: a + b)

    # To list
    digit_totals_list = digit_totals.collect()

    # Fetch most and least common
    most_comb = max(digit_totals_list, key=lambda x: x[1])
    least_comb = min(digit_totals_list, key=lambda x: x[1])

    return (most_comb[0], most_comb[1], least_comb[0], least_comb[1])

    # Output: a tuple (most common digit, most common frequency, least common digit, least common frequency)

units = ["", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"]
teens = ["ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen", "eighteen", "nineteen"]
tens = ["", "", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety"]

def under_thousand(num, units, teens, tens):
    if num == 0:
        return ""
    words = []
    if num >= 100:
        hundreds = num // 100
        if 1 <= hundreds < len(units):
            words += [units[hundreds], "hundred"]
        num %= 100
        if num > 0:
            words.append("and")
    if num >= 20:
        words.append(tens[num // 10])
        num %= 10
    if 10 <= num < 20:
        words.append(teens[num - 10])
    elif 0 < num < 10:
        words.append(units[num])
    return " ".join(filter(None, words))

def number_to_english(n, units, teens, tens):
    if n == 0:
        return "zero"
    if n == 1000000:
        return "one million"
    result = []
    thousands = n // 1000
    remainder = n % 1000
    if thousands > 0:
        result.append(under_thousand(thousands, units, teens, tens))
        result.append("thousand")
    if remainder > 0 or not result:
        result.append(under_thousand(remainder, units, teens, tens))
    return " ".join(result).strip()

def q7(rdd):
    # Input: the RDD from Q4
    if rdd.isEmpty():
        return (None, 0, None, 0)
    
    # Make into (key, value)
    rdd_pairs = rdd.map(lambda x: (x, x))
    
    # Map all numbers into string form, and into indiv. chars
    char_counts = general_map(rdd_pairs, lambda _, v: [(char, 1) for char in number_to_english(v, units, teens, tens) if char != " "])
    
    # Take totals for each char
    char_totals = general_reduce(char_counts, lambda a, b: a + b)

    # To list
    char_totals_list = char_totals.collect()
   
    # Fetch most and least common
    most_comb = max(char_totals_list, key=lambda x: x[1])
    least_comb = min(char_totals_list, key=lambda x: x[1])
    
    return (most_comb[0], most_comb[1], least_comb[0], least_comb[1])

    # Output: a tuple (most common char, most common frequency, least common char, least common frequency)

def general_map(rdd, f):
    """
    rdd: an RDD with values of type (k1, v1)
    f: a function (k1, v1) -> List[(k2, v2)]
    output: an RDD with values of type (k2, v2)
    """
    return rdd.flatMap(lambda pair: f(pair[0], pair[1]))

def general_reduce(rdd, f):
    """
    rdd: an RDD with values of type (k2, v2)
    f: a function (v2, v2) -> v2
    output: an RDD with values of type (k2, v2),
        and just one single value per key
    """
    return rdd.reduceByKey(f)

def PART_1_PIPELINE_PARAMETRIC(N, P):
    """
    TODO: Follow the same logic as PART_1_PIPELINE
    N = number of inputs
    P = parallelism (number of partitions)
    (You can copy the code here), but make the following changes:
    - load_input should use an input of size N.
    - load_input_bigger (including q8_a and q8_b) should use an input of size N.
    - both of these should return an RDD with level of parallelism P (number of partitions = P).
    """
    rdd1 = load_input(N=N, P=P)
    rdd2 = load_input_bigger(N=N, P=P)

    result1 = q8_a(N=N,P=P)
    result2 = q8_b(N=N,P=P)

    return

"""
=== Coding part 2: measuring the throughput and latency ===

Now we are ready to measure the throughput and latency.

To start, copy the code for ThroughputHelper and LatencyHelper from HW1 into this file.

Then, please measure the performance of PART1_PIPELINE as a whole
using five levels of parallelism:
- parallelism 1
- parallelism 2
- parallelism 4
- parallelism 8
- parallelism 16

For each level of parallelism, you should measure the throughput and latency as the number of input
items increases, using the following input sizes:
- N = 1, 10, 100, 1000, 10_000, 100_000, 1_000_000.

- Note that the larger sizes may take a while to run (for example, up to 30 minutes). You can try with smaller sizes to test your code first.

You can generate any plots you like (for example, a bar chart or an x-y plot on a log scale,)
but store them in the following 10 files,
where the file name corresponds to the level of parallelism:

output/part3-throughput-1.png
output/part3-throughput-2.png
output/part3-throughput-4.png
output/part3-throughput-8.png
output/part3-throughput-16.png
output/part3-latency-1.png
output/part3-latency-2.png
output/part3-latency-4.png
output/part3-latency-8.png
output/part3-latency-16.png

Clarifying notes:

- To control the level of parallelism, use the N, P parameters in your PART_1_PIPELINE_PARAMETRIC above.

- Make sure you sanity check the output to see if it matches what you would expect! The pipeline should run slower
  for larger input sizes N (in general) and for fewer number of partitions P (in general).

- For throughput, the "number of input items" should be 2 * N -- that is, N input items for load_input, and N for load_input_bigger.

- For latency, please measure the performance of the code on the entire input dataset
(rather than a single input row as we did on HW1).
MapReduce is batch processing, so all input rows are processed as a batch
and the latency of any individual input row is the same as the latency of the entire dataset.
That is why we are assuming the latency will just be the running time of the entire dataset.

- Set `NUM_RUNS` to `1` if you haven't already. Note that this will make the values for low numbers (like `N=1`, `N=10`, and `N=100`) vary quite unpredictably.
"""
import matplotlib.pyplot as plt

# Copy in ThroughputHelper and LatencyHelper
NUM_RUNS=1
class ThroughputHelper:
    def __init__(self):
        # Initialize the object.
        # Pipelines: a list of functions, where each function
        # can be run on no arguments.
        # (like: def f(): ... )
        self.pipelines = []

        # Pipeline names
        # A list of names for each pipeline
        self.names = []

        # Pipeline input sizes
        self.sizes = []

        # Pipeline throughputs
        # This is set to None, but will be set to a list after throughputs
        # are calculated.
        self.throughputs = None

    def add_pipeline(self, name, size, func):
        self.names.append(name)
        self.sizes.append(size)
        self.pipelines.append(func)

    def compare_throughput(self):
        self.throughputs = []
        # Measure the throughput of all pipelines
        for i in range(0,len(self.pipelines)):
            # Calculate total time
            start_time = time.perf_counter()
            N = self.sizes[i][0] if isinstance(self.sizes[i], list) else self.sizes[i]
            for _ in range(NUM_RUNS):
                self.pipelines[i](N)
            end_time = time.perf_counter()
            # Append to throughput
            self.throughputs.append((NUM_RUNS * N) / (end_time - start_time))
        print(f"self.throughputs: {self.throughputs}")
        return self.throughputs

    def generate_plot(self, filename):
        # Generate a plot for throughput using matplotlib.
        plt.figure(figsize=(10, 6))
        plt.bar(self.names, self.throughputs, color='skyblue')
        plt.xlabel('Pipelines')
        plt.ylabel('Throughput (runs per second)')
        plt.title('Throughput Comparison Across Pipelines')
        plt.xticks(rotation=45, ha='right')
        plt.legend()
        plt.tight_layout()
        plt.savefig(filename)
        plt.close()

class LatencyHelper:
    def __init__(self):
        # Initialize the object.
        # Pipelines: a list of functions, where each function
        # can be run on no arguments.
        # (like: def f(): ... )
        self.pipelines = []

        # Pipeline names
        # A list of names for each pipeline
        self.names = []

        # Pipeline latencies
        # This is set to None, but will be set to a list after latencies
        # are calculated.
        self.latencies = None

    def add_pipeline(self, name, func):
        self.names.append(name)
        self.pipelines.append(func)

    def compare_latency(self):
        self.latencies = []
        # Measure the latency of all pipelines
        for i in range(0,len(self.pipelines)):
            # Calculate total time
            start_time = time.time()
            for _ in range(NUM_RUNS):
                self.pipelines[i](10)
            end_time = time.time()
            # Append to latency
            self.latencies.append((end_time - start_time) * 1000)
        return self.latencies

    def generate_plot(self, filename):
        # Generate a plot for throughput using matplotlib.
        plt.figure(figsize=(10, 6))
        plt.bar(self.names, self.latencies, color='skyblue')
        plt.xlabel('Pipelines')
        plt.ylabel('Latency (Ms)')
        plt.title('Latency Comparison Across Pipelines')
        plt.xticks(rotation=45, ha='right')
        plt.legend()
        plt.tight_layout()
        plt.savefig(filename)
        plt.close()
# Insert code to generate plots here as needed
"""
=== Reflection part ===

Once this is done, write a reflection and save it in
a text file, output/part3-reflection.txt.

I would like you to think about and answer the following questions:

1. What would we expect from the throughput and latency
of the pipeline, given only the dataflow graph?

Use the information we have seen in class. In particular,
how should throughput and latency change when we double the amount of parallelism?

Please ignore pipeline and task parallelism for this question.
The parallelism we care about here is data parallelism.

2. In practice, does the expectation from question 1
match the performance you see on the actual measurements? Why or why not?

State specific numbers! What was the expected throughput and what was the observed?
What was the expected latency and what was the observed?

3. Finally, use your answers to Q1-Q2 to form a conjecture
as to what differences there are (large or small) between
the theoretical model and the actual runtime.
Name some overheads that might be present in the pipeline
that are not accounted for by our theoretical model of
dataflow graphs that could affect performance.

You should list an explicit conjecture in your reflection, like this:

    Conjecture: I conjecture that ....

You may have different conjectures for different parallelism cases.
For example, for the parallelism=4 case vs the parallelism=16 case,
if you believe that different overheads are relevant for these different scenarios.

=== Grading notes ===

Don't forget to fill out the entrypoint below before submitting your code!
Running python3 part3.py should work and should re-generate all of your plots in output/.

In the reflection, please write at least a paragraph for each question. (5 sentences each)

Please include specific numbers in your reflection (particularly for Q2).

=== Entrypoint ===
"""

if __name__ == '__main__':
    print("Complete part 3. Please use the main function below to generate your plots so that they are regenerated whenever the code is run:")

    # Input sizes and parallelism levels
    input_sizes = [1, 10, 100, 1000, 10_000, 100_000, 1_000_000]
    parallelisms = [1, 2, 4, 8, 16]

    for P in parallelisms:
        # Throughput measurement
        th = ThroughputHelper()
        for N in input_sizes:
            th.add_pipeline(
                name=f"N={N}",
                size=[N],
                func=lambda size, N=N, P=P: PART_1_PIPELINE_PARAMETRIC(N, P)
            )
        th.compare_throughput()
        th.generate_plot(f"output/part3-throughput-{P}.png")

        # Latency measurement
        lat = LatencyHelper()
        for N in input_sizes:
            lat.add_pipeline(
                name=f"N={N}",
                func=lambda N=N, P=P: PART_1_PIPELINE_PARAMETRIC(N, P)
            )
        lat.compare_latency()
        lat.generate_plot(f"output/part3-latency-{P}.png")
