# test_example.py

# This is a dummy test file for pytest.
# pytest automatically discovers files named test_*.py or *_test.py
# and functions within them named test_*.


def test_always_passes():
    """
    This is a simple test function.
    It asserts that True is True, which is always the case.
    When run by pytest, this test will always pass.
    """
    # The 'assert' keyword is used to check conditions in tests.
    # If the condition after 'assert' is False, the test fails.
    # If it's True (or no exception is raised), the test passes.
    assert True


# You can add more test functions in this file or create other test_*.py files.


# Example of another simple test:
def test_basic_arithmetic():
    """
    Another simple test checking basic arithmetic.
    """
    assert 1 + 1 == 2
    assert 5 * 2 != 9
