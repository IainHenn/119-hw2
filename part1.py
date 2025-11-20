"""
Part 1: MapReduce

In our first part, we will practice using MapReduce
to create several pipelines.
This part has 20 questions.

As you complete your code, you can run the code with

    python3 part1.py
    pytest part1.py

and you can view the output so far in:

    output/part1-answers.txt

In general, follow the same guidelines as in HW1!
Make sure that the output in part1-answers.txt looks correct.
See "Grading notes" here:
https://github.com/DavisPL-Teaching/119-hw1/blob/main/part1.py

For Q5-Q7, make sure your answer uses general_map and general_reduce as much as possible.
You will still need a single .map call at the beginning (to convert the RDD into key, value pairs), but after that point, you should only use general_map and general_reduce.

If you aren't sure of the type of the output, please post a question on Piazza.
"""

# Spark boilerplate (remember to always add this at the top of any Spark file)
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataflowGraphExample").getOrCreate()
sc = spark.sparkContext

# Additional imports
import pytest
import random

"""
===== Questions 1-3: Generalized Map and Reduce =====

We will first implement the generalized version of MapReduce.
It works on (key, value) pairs:

- During the map stage, for each (key1, value1) pairs we
  create a list of (key2, value2) pairs.
  All of the values are output as the result of the map stage.

- During the reduce stage, we will apply a reduce_by_key
  function (value2, value2) -> value2
  that describes how to combine two values.
  The values (key2, value2) will be grouped
  by key, then values of each key key2
  will be combined (in some order) until there
  are no values of that key left. It should end up with a single
  (key2, value2) pair for each key.

1. Fill in the general_map function
using operations on RDDs.

If you have done it correctly, the following test should pass.
(pytest part1.py)

Don't change the q1() answer. It should fill out automatically.
"""

"""
export PYSPARK_PYTHON="C:/Users/Iaine/.vscode/PYTHON~1/ecs119-hw/119-hw2/.venv310/Scripts/python.exe"
export PYSPARK_DRIVER_PYTHON="C:/Users/Iaine/.vscode/PYTHON~1/ecs119-hw/119-hw2/.venv310/Scripts/python.exe"
"""

def general_map(rdd, f):
    """
    rdd: an RDD with values of type (k1, v1)
    f: a function (k1, v1) -> List[(k2, v2)]
    output: an RDD with values of type (k2, v2)
    """
    return rdd.flatMap(lambda pair: f(pair[0], pair[1]))

# Remove skip when implemented!
#@pytest.mark.skip
def test_general_map():
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])

    # Use first character as key
    rdd1 = rdd.map(lambda x: (x[0], x))

    # Map returning no values
    rdd2 = general_map(rdd1, lambda k, v: [])

    # Map returning length
    rdd3 = general_map(rdd1, lambda k, v: [(k, len(v))])
    rdd4 = rdd3.map(lambda pair: pair[1])

    # Map returnning odd or even length
    rdd5 = general_map(rdd1, lambda k, v: [(len(v) % 2, ())])

    assert rdd2.collect() == []
    assert sum(rdd4.collect()) == 14
    assert set(rdd5.collect()) == set([(1, ())])

def q1():
    # Answer to this part: don't change this
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])
    rdd1 = rdd.map(lambda x: (x[0], x))
    rdd2 = general_map(rdd1, lambda k, v: [(1, v[-1])])
    return sorted(rdd2.collect())

"""
2. Fill in the reduce function using operations on RDDs.

If you have done it correctly, the following test should pass.
(pytest part1.py)

Don't change the q2() answer. It should fill out automatically.
"""

def general_reduce(rdd, f):
    """
    rdd: an RDD with values of type (k2, v2)
    f: a function (v2, v2) -> v2
    output: an RDD with values of type (k2, v2),
        and just one single value per key
    """
    return rdd.reduceByKey(f)

# Remove skip when implemented!
#@pytest.mark.skip
def test_general_reduce():
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])

    # Use first character as key
    rdd1 = rdd.map(lambda x: (x[0], x))

    # Reduce, concatenating strings of the same key
    rdd2 = general_reduce(rdd1, lambda x, y: x + y)
    res2 = set(rdd2.collect())

    # Reduce, adding lengths
    rdd3 = general_map(rdd1, lambda k, v: [(k, len(v))])
    rdd4 = general_reduce(rdd3, lambda x, y: x + y)
    res4 = sorted(rdd4.collect())

    assert (
        res2 == set([('c', "catcow"), ('d', "dog"), ('z', "zebra")])
        or res2 == set([('c', "cowcat"), ('d', "dog"), ('z', "zebra")])
    )
    assert res4 == [('c', 6), ('d', 3), ('z', 5)]

def q2():
    # Answer to this part: don't change this
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])
    rdd1 = rdd.map(lambda x: (x[0], x))
    rdd2 = general_reduce(rdd1, lambda x, y: "hello")
    return sorted(rdd2.collect())

"""
3. Name one scenario where having the keys for Map
and keys for Reduce be different might be useful.

=== ANSWER Q3 BELOW ===
Maybe in the map phase we have (Last Name, Score in %) for some student data
and then in the reduce phase we want to group by last name and average the score, 
and then map each of the last names to their parent's full name (guardian). 
=== END OF Q3 ANSWER ===
"""

"""
===== Questions 4-10: MapReduce Pipelines =====

Now that we have our generalized MapReduce function,
let's do a few exercises.
For the first set of exercises, we will use a simple dataset that is the
set of integers between 1 and 1 million (inclusive).

4. First, we need a function that loads the input.
"""

def load_input():

    # Return a parallelized RDD with the integers between 1 and 1,000,000
    return sc.parallelize(range(1, 1000001))

def q4(rdd):
    # Input: the RDD from load_input
    return rdd.count()
    # Output: the length of the dataset.

"""
Now use the general_map and general_reduce functions to answer the following questions.

5. Among the numbers from 1 to 1 million, what is the average value?
"""

def q5(rdd):
    # Input: the RDD from Q4
    return rdd.sum() / rdd.count() if rdd.count() > 0 else 0
    # Output: the average value

"""
6. Among the numbers from 1 to 1 million, when written out,
which digit is most common, with what frequency?
And which is the least common, with what frequency?

(If there are ties, you may answer any of the tied digits.)

The digit should be either an integer 0-9 or a character '0'-'9'.
Frequency is the number of occurences of each value.

Your answer should use the general_map and general_reduce functions as much as possible.
"""

def q6(rdd):
    # Input: the RDD from Q4

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

"""
7. Among the numbers from 1 to 1 million, written out in English, which letter is most common?
With what frequency?
The least common?
With what frequency?

(If there are ties, you may answer any of the tied characters.)

For this part, you will need a helper function that computes
the English name for a number.
Examples:

    0 = zero
    71 = seventy one
    513 = five hundred and thirteen
    801 = eight hundred and one
    999 = nine hundred and ninety nine
    1001 = one thousand one
    500,501 = five hundred thousand five hundred and one
    555,555 = five hundred and fifty five thousand five hundred and fifty five
    1,000,000 = one million

Notes:
- For "least frequent", count only letters which occur,
  not letters which don't occur.
- Please ignore spaces and hyphens.
- Use lowercase letters.
- The word "and" should always appear after the "hundred" part (where present),
  but nowhere else.
  (Note the 1001 case above which differs from some other implementations.)
- Please implement this without using an external library such as `inflect`.
"""

# *** Define helper function(s) here ***
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

"""
8. Does the answer change if we have the numbers from 1 to 100,000,000?

Make a version of both pipelines from Q6 and Q7 for this case.
You will need a new load_input function.
"""

def load_input_bigger():
    # Return a parallelized RDD with the integers between 1 and 100,000,000
    return sc.parallelize(range(1, 100000001))

def q8_a():
    rdd = load_input_bigger()
    return q6(rdd)

def q8_b():
    rdd = load_input_bigger()
    return q7(rdd)

"""
Discussion questions

9. State the values of k1, v1, k2, and v2 for your Q6 and Q7 pipelines.

=== ANSWER Q9 BELOW ===
q6: ('1', 600001, '0', 488895)
q7: ('n', 7141002, 'm', 1)
=== END OF Q9 ANSWER ===

10. Do you think it would be possible to compute the above using only the
"simplified" MapReduce we saw in class? Why or why not?

=== ANSWER Q10 BELOW ===
If we're talking about the simplified MapReduce that was:
(input) ---> (map) ---> (reduce)

Then I don't think so, we're using some intermediary functions 
that are necessary in mapping and reducing, but if we ignore that then
yes this simplified model would work. 
=== END OF Q10 ANSWER ===
"""

"""
===== Questions 11-18: MapReduce Edge Cases =====

For the remaining questions, we will explore two interesting edge cases in MapReduce.

11. One edge case occurs when there is no output for the reduce stage.
This can happen if the map stage returns an empty list (for all keys).

Demonstrate this edge case by creating a specific pipeline which uses
our data set from Q4. It should use the general_map and general_reduce functions.

Output a set of (key, value) pairs after the reduce stage.
"""

def q11(rdd):
    # Input: the RDD from Q4
    rdd_pairs = rdd.map(lambda x: (x, x))

    # Map all numbers to an empty list
    rdd_map = general_map(rdd_pairs, lambda _, v: [])

    # Reduce the map
    rdd_reduce = general_reduce(rdd_map, lambda _, v: [])

    # Output: the result of the pipeline, a set of (key, value) pairs
    return set(rdd_reduce.collect())

"""
12. What happened? Explain below.
Does this depend on anything specific about how
we chose to define general_reduce?

=== ANSWER Q12 BELOW ===
It returned an empty list, not that I believe,
because the rdd_map was already empty, so I don't think
a different definition of general_reduce would've 
changed anything as it uses the map that was blank.
=== END OF Q12 ANSWER ===

13. Lastly, we will explore a second edge case, where the reduce stage can
output different values depending on the order of the input.
This leads to something called "nondeterminism", where the output of the
pipeline can even change between runs!

First, take a look at the definition of your general_reduce function.
Why do you imagine it could be the case that the output of the reduce stage
is different depending on the order of the input?

=== ANSWER Q13 BELOW ===
Let's say you're running a specific set of order of operations on a list:
[-1,19,-2,4,7]
with operations [*,/,-,+]
where an operation happens between two numbers in
the list, if we change the order of the numbers in the list, the result should 
change. Simpler idea is if we subtracted 3 numbers, changing the numbers and running
a reduce would change the final result.
=== END OF Q13 ANSWER ===

14.
Now demonstrate this edge case concretely by writing a specific example below.
As before, you should use the same dataset from Q4.

Important: Please create an example where the output of the reduce stage is a set of (integer, integer) pairs.
(So k2 and v2 are both integers.)
"""

def q14(rdd):
    # Input: the RDD from Q4
    rdd_pairs = rdd.map(lambda x: (x, x if random.random() < 0.5 else -x))

    rdd_reduce = general_reduce(rdd_pairs, lambda x, y: x - y)
    result = set(rdd_reduce.collect())
    return result

"""
15.
Run your pipeline. What happens?
Does it exhibit nondeterministic behavior on different runs?
(It may or may not! This depends on the Spark scheduler and implementation,
including partitioning.

=== ANSWER Q15 BELOW ===
Numbers look different every time, or atleast in a different 
order every time, but I can't really tell because of how many
pairs there are!
=== END OF Q15 ANSWER ===

16.
Lastly, try the same pipeline as in Q14
with at least 3 different levels of parallelism.

Write three functions, a, b, and c that use different levels of parallelism.
"""

def q16_a():
    # 2 partitions
    rdd = sc.parallelize(range(1, 1000001), 1)
    rdd_pairs = rdd.map(lambda x: (x, x if random.random() < 0.5 else -x))
    rdd_reduce = general_reduce(rdd_pairs, lambda x, y: x - y)
    return set(rdd_reduce.collect())

def q16_b():
    # 8 partitions
    rdd = sc.parallelize(range(1, 1000001), 12)
    rdd_pairs = rdd.map(lambda x: (x, x if random.random() < 0.5 else -x))
    rdd_reduce = general_reduce(rdd_pairs, lambda x, y: x - y)
    return set(rdd_reduce.collect())

def q16_c():
    # 32 partitions
    rdd = sc.parallelize(range(1, 1000001), 4)
    rdd_pairs = rdd.map(lambda x: (x, x if random.random() < 0.5 else -x))
    rdd_reduce = general_reduce(rdd_pairs, lambda x, y: x - y)
    return set(rdd_reduce.collect())

"""
Discussion questions

17. Was the answer different for the different levels of parallelism?

=== ANSWER Q17 BELOW ===
Yes! 16c specifically was the most different.
=== END OF Q17 ANSWER ===

18. Do you think this would be a serious problem if this occured on a real-world pipeline?
Explain why or why not.

=== ANSWER Q18 BELOW ===
Probably, if you were doing any sort of 
transformations I could see this as an issue.
=== END OF Q18 ANSWER ===

===== Q19-20: Further reading =====

19.
The following is a very nice paper
which explores this in more detail in the context of real-world MapReduce jobs.
https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/icsecomp14seip-seipid15-p.pdf

Take a look at the paper. What is one sentence you found interesting?

=== ANSWER Q19 BELOW ===
"We find five bugs in well-tested production programs caused by noncommutative reducers. 
Their root causes include wrong assumptions
on implicit data properties, data corruption, and various semantic
errors leading to non-commutativity."

This is nuts!
=== END OF Q19 ANSWER ===

20.
Take one example from the paper, and try to implement it using our
general_map and general_reduce functions.
For this part, just return the answer "True" at the end if you found
it possible to implement the example, and "False" if it was not.
"""

def q20():
    # The FirstN pattern: collect the first N num of names, and join
    rdd = sc.parallelize(range(1, 11))

    rdd_pairs = rdd.map(lambda x: (1, str(x)))

    result_rdd = general_reduce(rdd_pairs, lambda a, b: a + " " + b)
    
    result = result_rdd.collect()

    return True if result else False

"""
That's it for Part 1!

===== Wrapping things up =====

**Don't modify this part.**

To wrap things up, we have collected
everything together in a pipeline for you below.

Check out the output in output/part1-answers.txt.
"""

ANSWER_FILE = "output/part1-answers.txt"
UNFINISHED = 0

def log_answer(name, func, *args):
    try:
        answer = func(*args)
        print(f"{name} answer: {answer}")
        with open(ANSWER_FILE, 'a') as f:
            f.write(f'{name},{answer}\n')
            print(f"Answer saved to {ANSWER_FILE}")
    except NotImplementedError:
        print(f"Warning: {name} not implemented.")
        with open(ANSWER_FILE, 'a') as f:
            f.write(f'{name},Not Implemented\n')
        global UNFINISHED
        UNFINISHED += 1

def PART_1_PIPELINE():
    open(ANSWER_FILE, 'w').close()

    try:
        dfs = load_input()
    except NotImplementedError:
        print("Welcome to Part 1! Implement load_input() to get started.")
        dfs = sc.parallelize([])

    # Questions 1-3
    log_answer("q1", q1)
    log_answer("q2", q2)
    # 3: commentary

    # Questions 4-10
    log_answer("q4", q4, dfs)
    log_answer("q5", q5, dfs)
    log_answer("q6", q6, dfs)
    log_answer("q7", q7, dfs)
    log_answer("q8a", q8_a)
    log_answer("q8b", q8_b)
    # 9: commentary
    # 10: commentary

    # Questions 11-18
    log_answer("q11", q11, dfs)
    # 12: commentary
    # 13: commentary
    log_answer("q14", q14, dfs)
    # 15: commentary
    log_answer("q16a", q16_a)
    log_answer("q16b", q16_b)
    log_answer("q16c", q16_c)
    # 17: commentary
    # 18: commentary

    # Questions 19-20
    # 19: commentary
    log_answer("q20", q20)

    # Answer: return the number of questions that are not implemented
    if UNFINISHED > 0:
        print("Warning: there are unfinished questions.")

    return f"{UNFINISHED} unfinished questions"

if __name__ == '__main__':
    log_answer("PART 1", PART_1_PIPELINE)

"""
=== END OF PART 1 ===
"""
