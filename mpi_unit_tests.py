import unittest
from mpi_simulator import FindMax, FindMean, FindMed, FindMin
import numpy as np

class TestFunctions(unittest.TestCase):
    def test_min_fun(self):
        test_data = [1,2,3]
        min = np.min(test_data)
        #print(min)
        task = FindMin(test_data)
        self.assertEqual(task.execute(),min)

    def test_max_fun(self):
        test_data = [1,2,3]
        max = np.max(test_data)
        #print(max)
        task = FindMax(test_data)
        self.assertEqual(task.execute(),max)
    
    def test_med_fun(self):
        test_data = [1,2,3]
        med = np.median(test_data)
        #print(med)
        task = FindMed(test_data)
        self.assertEqual(task.execute(),med)

    def test_mean_fun(self):
        test_data = [1,2,3]
        mean = np.mean(test_data)
        #print(mean)
        task = FindMean(test_data)
        self.assertEqual(task.execute(),mean)


if __name__ == "__main__":
    unittest.main()

