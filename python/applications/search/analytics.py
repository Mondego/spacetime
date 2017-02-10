import os.path
import pickle
from collections import defaultdict


def singleton(cls):
    instances = {}

    def get_instance():
        if cls not in instances:
            instances[cls] = cls()
        return instances[cls]
    return get_instance


@singleton
class Analytics:
    def __init__(self):
        '''
        Constructor for the analytics class either reads from an analytics file which has persisten analytics
        values, if not it creates an empty dict() to record analytics
        '''
        self.filename = 'analytics.dat'
        self.valid_labels = ['DOMAINS', 'SUB_DOMAINS', 'PATHS', 'MAX_OUT_LINKS', 'INVALID_LINKS']
        if os.path.isfile(self.filename):
            with open(self.filename, 'rb') as file_handle:
                self.analytics = pickle.load(file_handle)
        else:
            self.analytics = {
                'DOMAINS': defaultdict(int),
                'SUB_DOMAINS': defaultdict(int),
                'PATHS': defaultdict(int),
                'MAX_OUT_LINKS': (None, -1),
                'INVALID_LINKS': []
            }

    def merge(self, label, metric):
        '''
        This function is used to merge the different parameters in the analytics object with new values
        these can be one of the following DOMAINS, SUB_DOMAINS, PATHS, MAX_OUT_LINKS, INVALID_LINKS
        :param label: Is one among the following pieces of text - DOMAINS, SUB-DOMAINS, PATHS, MAX_OUT_LINKS,
                        INVALID_LINKS
        :param metric: The object that describes the label
        :return: NA
        '''

        # Updating all the different analytics metrics
        if label == 'DOMAINS' or label == 'SUB_DOMAINS' or label == 'PATHS':
            # Combining the domains', sub-domains', paths' dictionaries
            for value in metric:
                self.analytics[label][value] += metric[value]
        elif label == 'MAX_OUT_LINKS':
            # Checking which URL has highest outgoing links and then saving the appropriate one
            if self.analytics[label][1] < metric[1]:
                self.analytics[label] = metric
        elif label == 'INVALID_LINKS':
            self.analytics[label] += metric

    def set(self, label, metric):
        '''
        This function is used to set the different parameters in the analytics object; these can be one of the following
        DOMAINS, SUB_DOMAINS, PATHS, MAX_OUT_LINKS, INVALID_LINKS
        :param label: Is one among the following pieces of text - DOMAINS, SUB-DOMAINS, PATHS, MAX_OUT_LINKS,
                        INVALID_LINKS
        :param metric: The object that describes the label
        :return: NA
        '''
        if label in self.valid_labels:
            self.analytics[label] = metric

    def get(self, label):
        '''
        This function is used to get the different parameters in the analytics object; these can be one of the following
        DOMAINS, SUB_DOMAINS, PATHS, MAX_OUT_LINKS, INVALID_LINKS
        :param label: Is one among the following pieces of text - DOMAINS, SUB-DOMAINS, PATHS, MAX_OUT_LINKS,
                        INVALID_LINKS
        :return: The metric associated with the label
        '''
        if label in self.valid_labels:
            return self.analytics[label]
        else:
            return None

    def write_to_file(self):
        '''
        Writes the analytics function to a file for persistence
        :return: NA
        '''
        with open(self.filename, 'wb') as file_handle:
            pickle.dump(self.analytics, file_handle, protocol=pickle.HIGHEST_PROTOCOL)

a = Analytics()
print a.analytics