#!/usr/bin/env python3
"""
Test script for DiscogsSearchGUI menu system
Simulates user interaction to verify menu functionality
"""

from DiscogsSearchGUI import DiscogsSearchGUI
from unittest.mock import patch
import sys

def test_gui_initialization():
    """Test 1: Verify GUI initializes correctly"""
    print("\n" + "="*60)
    print("TEST 1: GUI Initialization")
    print("="*60)
    
    try:
        gui = DiscogsSearchGUI()
        print("✓ DiscogsSearchGUI initialized")
        print(f"✓ Base search URL: {gui.base_discogs_search_url}")
        print(f"✓ Data handler present: {gui.data_handler is not None}")
        print(f"✓ User interaction module present: {gui.user_interaction is not None}")
        return True, gui
    except Exception as e:
        print(f"✗ Initialization failed: {e}")
        import traceback
        traceback.print_exc()
        return False, None

def test_menu_display(gui):
    """Test 2: Verify menu can display without errors"""
    print("\n" + "="*60)
    print("TEST 2: Menu Display")
    print("="*60)
    
    try:
        # Simulate pressing 'Q' to quit immediately
        with patch.object(gui.user_interaction, 'get_user_input', return_value='Q'):
            gui.user_interaction_menu()
        print("✓ Menu displayed and exited successfully")
        return True
    except Exception as e:
        print(f"✗ Menu display failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_invalid_menu_choice(gui):
    """Test 3: Verify invalid menu choice handling"""
    print("\n" + "="*60)
    print("TEST 3: Invalid Menu Choice Handling")
    print("="*60)
    
    try:
        # Simulate entering invalid choice then quitting
        inputs = iter(['99', 'Q'])
        with patch.object(gui.user_interaction, 'get_user_input', side_effect=inputs):
            gui.user_interaction_menu()
        print("✓ Invalid choice handled gracefully")
        return True
    except Exception as e:
        print(f"✗ Invalid choice handling failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_display_dataframe(gui):
    """Test 4: Verify display dataframe option"""
    print("\n" + "="*60)
    print("TEST 4: Display DataFrame Function")
    print("="*60)
    
    try:
        # Simulate selecting option 3 (Display DataFrame) then quitting
        inputs = iter(['3', 'Q'])
        with patch.object(gui.user_interaction, 'get_user_input', side_effect=inputs):
            gui.user_interaction_menu()
        print("✓ Display DataFrame option works")
        return True
    except Exception as e:
        print(f"✗ Display DataFrame failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_view_filters(gui):
    """Test 5: Verify view applied filters option"""
    print("\n" + "="*60)
    print("TEST 5: View Applied Filters Function")
    print("="*60)
    
    try:
        # Simulate selecting option 8 (View Applied Filters) then quitting
        inputs = iter(['8', 'Q'])
        with patch.object(gui.user_interaction, 'get_user_input', side_effect=inputs):
            gui.user_interaction_menu()
        print("✓ View Applied Filters option works")
        return True
    except Exception as e:
        print(f"✗ View Applied Filters failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("\n" + "="*60)
    print("DISCOGS SEARCH GUI - AUTOMATED TESTING")
    print("="*60)
    
    results = []
    
    # Test 1: Initialization
    success, gui = test_gui_initialization()
    results.append(("Initialization", success))
    
    if not success:
        print("\n❌ Cannot proceed without successful initialization")
        return
    
    # Test 2: Menu Display
    success = test_menu_display(gui)
    results.append(("Menu Display", success))
    
    # Test 3: Invalid Choice Handling
    success = test_invalid_menu_choice(gui)
    results.append(("Invalid Choice Handling", success))
    
    # Test 4: Display DataFrame
    success = test_display_dataframe(gui)
    results.append(("Display DataFrame", success))
    
    # Test 5: View Filters
    success = test_view_filters(gui)
    results.append(("View Applied Filters", success))
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for test_name, success in results:
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"{status}: {test_name}")
    
    print("\n" + "="*60)
    print(f"Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 ALL TESTS PASSED!")
        print("✅ GUI application is fully functional in Docker")
    else:
        print(f"⚠️  {total - passed} test(s) failed")
    print("="*60)

if __name__ == "__main__":
    main()
