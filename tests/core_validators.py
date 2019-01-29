# coding=utf-8
import unittest

from maio.core.validators import (SimpleValidator, Val, _ERR_STR_NOT_ENDS_WITH, _ERR_STR_NOT_STARTS_WITH, _ERR_STR_TOO_SHORT, _ERR_STR_TOO_LONG)


class ValidatorMethods(unittest.TestCase):

    def test_required(self):
        validators = {
            'a': (Val.required(),),
            'b': []
        }

        data1 = {
            'a': 1,
            'b': 2,
            'c': 3
        }
        data2 = {
            'a': None,
            'b': 2,
            'c': 3
        }

        res1 = SimpleValidator.validate(data1, validators)
        res2 = SimpleValidator.validate(data2, validators)

        self.assertFalse(res1.has_errors())
        self.assertIn('a', res1.result)
        self.assertIn('b', res1.result)
        self.assertNotIn('c', res1.result)

        self.assertTrue(res2.has_errors())
        self.assertIn('a', res2.errors)

    def test_string(self):
        validators = {
            'a': (Val.required(), Val.string(min_len=3, max_len=6, ends_with='a', starts_with='a'))
        }

        data_ok = {'a': 'a1aa'}
        data_wrong_ending = {'a': 'a1b'}
        data_wrong_begining = {'a': 'b1a'}
        data_wrong_length1 = {'a': 'aa'}
        data_wrong_length2 = {'a': 'abbbbbbbba'}

        res1 = SimpleValidator.validate(data_ok, validators)
        self.assertFalse(res1.has_errors())

        res1 = SimpleValidator.validate(data_wrong_ending, validators)
        self.assertTrue(res1.has_errors())
        self.assertEqual(_ERR_STR_NOT_ENDS_WITH, res1.errors['a'][0]['code'])

        res1 = SimpleValidator.validate(data_wrong_begining, validators)
        self.assertTrue(res1.has_errors())
        self.assertEqual(_ERR_STR_NOT_STARTS_WITH, res1.errors['a'][0]['code'])

        res1 = SimpleValidator.validate(data_wrong_length1, validators)
        self.assertTrue(res1.has_errors())
        self.assertEqual(_ERR_STR_TOO_SHORT, res1.errors['a'][0]['code'])

        res1 = SimpleValidator.validate(data_wrong_length2, validators)
        self.assertTrue(res1.has_errors())
        self.assertEqual(_ERR_STR_TOO_LONG, res1.errors['a'][0]['code'])
