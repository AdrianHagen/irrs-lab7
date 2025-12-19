"""
.. module:: MRKmeansDef

MRKmeansDef
*************

:Description: MRKmeansDef

    

:Authors: bejar
    

:Version: 

:Created on: 17/07/2017 7:42 

"""

from collections import defaultdict
from mrjob.job import MRJob
from mrjob.step import MRStep

__author__ = 'bejar'


class MRKmeansStep(MRJob):
    prototypes = {}

    def jaccard(self, prot, doc):
        """
        Compute here the Jaccard similarity between a prototype and a document
        prot should be a list of pairs (word, probability)
        doc should be a list of words
        Words must be alphabeticaly ordered

        The result should be always a value in the range [0,1]
        """
        doc_norm_sq = float(len(doc))

        prot_norm_sq = sum([val ** 2 for _, val in prot])

        dot_product = 0.0
        i = 0
        j = 0
        while i < len(prot) and j < len(doc):
            prot_word = prot[i][0]
            prot_val = prot[i][1]
            doc_word = doc[j]

            if prot_word == doc_word:
                dot_product += prot_val  # Weighted intersection
                i += 1
                j += 1
            elif prot_word < doc_word:
                i += 1
            else:
                j += 1

        denominator = doc_norm_sq + prot_norm_sq - dot_product

        if denominator == 0:
            return 0.0
        return dot_product / denominator

    def configure_args(self):
        """
        Additional configuration flag to get the prototypes files

        :return:
        """
        super(MRKmeansStep, self).configure_args()
        self.add_file_arg('--prot')

    def load_data(self):
        """
        Loads the current cluster prototypes

        :return:
        """
        f = open(self.options.prot, 'r')
        for line in f:
            cluster, words = line.split(':')
            cp = []
            for word in words.split():
                cp.append((word.split('+')[0], float(word.split('+')[1])))
            self.prototypes[cluster] = cp

    def assign_prototype(self, _, line):
        """
        This is the mapper it should compute the closest prototype to a document

        Words should be sorted alphabetically in the prototypes and the documents

        This function has to return at list of pairs (prototype_id, document words)

        You can add also more elements to the value element, for example the document_id
        """

        # Each line is a string docid:wor1 word2 ... wordn
        doc, words = line.split(':')
        lwords = words.split()

        # Compute map
        best_cluster = None
        best_sim = -1.0
        for cluster in self.prototypes:
            sim = self.jaccard(self.prototypes[cluster], lwords)
            if sim > best_sim:
                best_sim = sim
                best_cluster = cluster
        # Yield the best cluster and the document words
        yield best_cluster, lwords



    def aggregate_prototype(self, key, values):
        """
        input is cluster and all the documents it has assigned
        Outputs should be at least a pair (cluster, new prototype)

        It should receive a list with all the words of the documents assigned for a cluster

        The value for each word has to be the frequency of the word divided by the number
        of documents assigned to the cluster

        Words are ordered alphabetically but you will have to use an efficient structure to
        compute the frequency of each word

        :param key: cluster id
        :param values: list of documents assigned to the cluster
        :return: cluster id and new prototype
        """

        word_count = defaultdict(int)
        doc_count = 0

        for doc_words in values:
            doc_count += 1
            for word in doc_words:
                word_count[word] += 1

        # Create new prototype as a sorted list of (word, frequency/doc_count)
        new_prototype = []
        for word in sorted(word_count.keys()):
            frequency = word_count[word] / float(doc_count)
            new_prototype.append(f"{word}+{frequency:.6f}")

        yield key, ' '.join(new_prototype)


    def steps(self):
        return [MRStep(mapper_init=self.load_data, mapper=self.assign_prototype,
                       reducer=self.aggregate_prototype)
            ]


if __name__ == '__main__':
    MRKmeansStep.run()