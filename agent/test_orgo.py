"""
Test script to verify Orgo Computer connection.
Run with: python test_orgo.py
"""
from orgo import Computer
import os
import json
from dotenv import load_dotenv

load_dotenv(dotenv_path="../.env")

print("Testing Orgo Computer connection...")
print("-" * 40)

computer_id = os.environ.get("ORGO_COMPUTER_ID")
if computer_id:
    print(f"Using existing computer ID: {computer_id}")
    computer = Computer(computer_id=computer_id)
else:
    print("Creating new computer...")
    computer = Computer()

print(f"\nURL: {computer.url}")

# Get full computer details from API
print(f"\nFetching computer details from API...")
try:
    details = computer.api.get_computer(computer.computer_id)
    print(json.dumps(details, indent=2))
except Exception as e:
    print(f"Error: {e}")

print("-" * 40)
