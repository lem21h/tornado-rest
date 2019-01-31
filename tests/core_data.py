# coding=utf-8
import unittest

from maio.core.data import VO


class DataUnitTests(unittest.TestCase):
    class TestVo(VO):
        __slots__ = ('a', 'b', 'c')

        def __init__(self) -> None:
            super().__init__()
            self.a = None
            self.b = None
            self.c = None

    def test_vo_create(self):
        obj = self.TestVo.create()

        self.assertIsInstance(obj, self.TestVo)
        self.assertIsNone(obj.a)
        self.assertIsNone(obj.b)
        self.assertIsNone(obj.c)

        self.assertListEqual(list(obj.get_fields()), ['a', 'b', 'c'])

    def test_vo_from_dict(self):
        # prepare data
        data = {'a': 1, 'b': 2, 'c': 3}

        # execute method
        obj = self.TestVo.from_dict(data)

        # check
        self.assertIsInstance(obj, self.TestVo)
        self.assertEqual(obj.a, data['a'])
        self.assertEqual(obj.b, data['b'])
        self.assertEqual(obj.c, data['c'])

    def test_vo_to_dict(self):
        # prepare data
        data = {'a': 1, 'b': 2, 'c': 3}
        obj = self.TestVo.from_dict(data)

        # execute method
        out1 = obj.to_dict()
        out2 = obj.to_dict(fields=('a', 'c'))

        # check
        self.assertIsInstance(out1, dict)
        self.assertDictEqual(data, out1)

        self.assertIsInstance(out2, dict)
        self.assertIn('a', out2)
        self.assertIn('c', out2)
        self.assertNotIn('b', out2)

    def test_vo_update(self):
        # prepare data
        data = {'a': 1, 'b': 2, 'c': 3}
        obj = self.TestVo.from_dict(data)
        obj1 = self.TestVo.from_dict(data)

        # execute method
        obj.update_object({'a': 10, 'b': 20})
        obj1.update_object({'a': 10, 'b': 20, 'd': 4})

        # check
        self.assertEqual(obj.a, 10)
        self.assertEqual(obj.b, 20)
        self.assertEqual(obj.c, 3)
        try:
            a = obj1.d
            self.fail("VO should not have extra fields added by updating it")
        except Exception:
            pass

    def test_vo_compare(self):
        # prepare data
        data1 = {'a': 1, 'b': 2, 'c': 3}
        data2 = {'a': 1, 'b': 3, 'c': 2}

        # execute method
        obj1 = self.TestVo.from_dict(data1)
        obj2 = self.TestVo.from_dict(data1)
        obj3 = self.TestVo.from_dict(data2)

        # check
        self.assertTrue(obj1 == obj2)
        self.assertTrue(obj1 == data1)
        self.assertFalse(obj1 == obj3)
        self.assertFalse(obj1 == data2)
