#!/usr/bin/env python3
"""
Test runner for the ETL pipeline.
Runs all unit tests and provides a summary report.
"""

import unittest
import sys
import os

# Add the parent directory to the path so we can import the etl modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def cleanup_test_files():
    """Clean up test files that might be left behind."""
    test_files = ['etl_test.db', 'etl.log']
    for file in test_files:
        if os.path.exists(file):
            try:
                os.remove(file)
                print(f"🧹 Cleaned up: {file}")
            except Exception as e:
                print(f"⚠️  Could not clean up {file}: {e}")

def run_all_tests():
    """Run all tests and return the test suite."""
    # Discover and load all tests
    loader = unittest.TestLoader()
    start_dir = os.path.dirname(os.path.abspath(__file__))
    suite = loader.discover(start_dir, pattern='test_*.py')
    return suite

def run_specific_test(test_name):
    """Run a specific test module."""
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromName(f'tests.{test_name}')
    return suite

def main():
    """Main test runner function."""
    print("🧪 ETL Pipeline Test Suite")
    print("=" * 50)
    
    # Clean up any leftover test files
    cleanup_test_files()
    
    # Check if a specific test was requested
    if len(sys.argv) > 1:
        test_name = sys.argv[1]
        print(f"Running specific test: {test_name}")
        suite = run_specific_test(test_name)
    else:
        print("Running all tests...")
        suite = run_all_tests()
    
    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "=" * 50)
    print("📊 Test Summary")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    if result.failures:
        print("\n❌ Failures:")
        for test, traceback in result.failures:
            print(f"  - {test}: {traceback}")
    
    if result.errors:
        print("\n💥 Errors:")
        for test, traceback in result.errors:
            print(f"  - {test}: {traceback}")
    
    # Clean up test files after tests
    cleanup_test_files()
    
    if result.wasSuccessful():
        print("\n✅ All tests passed!")
        return 0
    else:
        print("\n❌ Some tests failed!")
        return 1

if __name__ == '__main__':
    sys.exit(main()) 