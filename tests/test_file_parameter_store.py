from pathlib import Path
from unittest import TestCase

from pyhouse.paramstore.file_parameter_store import FileParameterStore


class TestFileParameterStore(TestCase):
    store_name: Path = Path(__file__).with_name("parameter_store.ini")

    def setUp(self):
        self.store = FileParameterStore(self.store_name)

    def tearDown(self):
        try:
            self.store_name.unlink()
        except FileNotFoundError:
            pass

    def test_get_param(self):
        # ensure no file is present
        self.tearDown()
        with self.assertRaises(KeyError):
            self.store.get_param("non-existing-key-in-non-existing-file")

        self.store.set_param("foo", "bar")
        self.assertEqual("bar", self.store.get_param("foo"))

        with self.assertRaises(KeyError):
            self.store.get_param("non-existing-key-in-existing-file")

    def test_set_param(self):
        self.store.set_param(param="foo", value="bar")
        # keys can be overwritten
        self.store.set_param("foo", "baz", overwrite=True)
        # multiple keys can be stored
        self.store.set_param("frobniz", "bar")
        self.assertEqual("baz", self.store.get_param("foo"))
        self.assertEqual("bar", self.store.get_param("frobniz"))
