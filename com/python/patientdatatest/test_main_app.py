from unittest import TestCase
from com.python.patientdata.main_app import MainApp


class TestMainApp(TestCase):
    def test_clean_data(self):
        mainapp = MainApp()
        self.assertEqual(MainApp.clean_data(mainapp, "2.55"), 2.55)
        self.assertEqual(MainApp.clean_data(mainapp, "120"), 120)
        self.assertEqual(MainApp.clean_data(mainapp, "n/a"), 0.0)
        self.assertEqual(MainApp.clean_data(mainapp, None), 0.0)

    def test_diabetes_indicator(self):
        mainapp = MainApp()
        self.assertEqual(MainApp.diabetes_indicator(mainapp, 100), "normal")
        self.assertEqual(MainApp.diabetes_indicator(mainapp, 150), "prediabetes")
        self.assertEqual(MainApp.diabetes_indicator(mainapp, 201), "diabetes")

    def test_mask_data(self):
        mainapp = MainApp()
        self.assertEqual(MainApp.mask_data(mainapp, "sdsds"), "******")
