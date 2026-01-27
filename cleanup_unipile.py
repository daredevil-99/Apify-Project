# cleanup_unipile.py
"""
Script to identify and clean up duplicate Unipile accounts
Run this to fix the duplicate webhook issue
"""

import asyncio
import logging
from db_config import (
    list_all_unipile_accounts,
    delete_unipile_account,
    cleanup_duplicate_accounts,
    audit_client_accounts
)

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s:%(name)s:%(message)s'
)

logger = logging.getLogger(__name__)


async def main():
    print("=" * 70)
    print("🧹 UNIPILE ACCOUNT CLEANUP TOOL")
    print("=" * 70)
    print()
    
    # Step 1: List all accounts
    print("📋 Step 1: Listing all Unipile accounts...")
    accounts = await list_all_unipile_accounts()
    
    if not accounts:
        print("❌ No accounts found or error occurred")
        return
    
    print()
    print("=" * 70)
    print("📊 FOUND ACCOUNTS:")
    print("=" * 70)
    
    instagram_accounts = []
    linkedin_accounts = []
    
    for i, acc in enumerate(accounts, 1):
        acc_type = acc.get('type', 'UNKNOWN')
        acc_id = acc.get('id', 'N/A')
        username = acc.get('username', 'N/A')
        status = acc.get('status', 'N/A')
        
        print(f"{i}. {acc_type}")
        print(f"   ID: {acc_id}")
        print(f"   Username: {username}")
        print(f"   Status: {status}")
        print()
        
        if acc_type == 'INSTAGRAM':
            instagram_accounts.append(acc_id)
        elif acc_type == 'LINKEDIN':
            linkedin_accounts.append(acc_id)
    
    # Step 2: Check for duplicates
    print("=" * 70)
    print("🔍 Step 2: Checking for duplicates...")
    print("=" * 70)
    
    # ✅ Handle Instagram duplicates
    if len(instagram_accounts) > 1:
        print(f"⚠️ WARNING: Found {len(instagram_accounts)} Instagram accounts!")
        print(f"   Account IDs: {instagram_accounts}")
        print()
        print("   You should only have ONE Instagram account per Unipile instance.")
        print("   The duplicates are causing webhook confusion.")
        print()
        
        print("   Which account ID do you want to KEEP? (paste the ID)")
        print("   Usually the most recently connected one.")
        keep_id = input("   Keep: ").strip()
        
        if keep_id in instagram_accounts:
            delete_ids = [acc_id for acc_id in instagram_accounts if acc_id != keep_id]
            
            print()
            print(f"✅ Will keep: {keep_id}")
            print(f"🗑️  Will delete: {', '.join(delete_ids)}")
            print()
            
            confirm = input("   Proceed with deletion? (yes/no): ").strip().lower()
            
            if confirm == 'yes':
                result = await cleanup_duplicate_accounts(keep_id, delete_ids)
                print()
                print("=" * 70)
                print("✅ CLEANUP COMPLETE")
                print("=" * 70)
                print(f"   Kept: {result['kept']}")
                print(f"   Deleted: {result['deleted']}")
                print(f"   Failed: {result['failed']}")
            else:
                print("❌ Cleanup cancelled")
        else:
            print("❌ Invalid account ID")
    else:
        print("✅ No Instagram duplicates found")
    
    # ✅ Handle LinkedIn duplicates
    if len(linkedin_accounts) > 1:
        print()
        print(f"⚠️ WARNING: Found {len(linkedin_accounts)} LinkedIn accounts!")
        print(f"   Account IDs: {linkedin_accounts}")
        print()
        print("   Which account ID do you want to KEEP? (paste the ID)")
        keep_id = input("   Keep: ").strip()
        
        if keep_id in linkedin_accounts:
            delete_ids = [acc_id for acc_id in linkedin_accounts if acc_id != keep_id]
            
            print()
            print(f"✅ Will keep: {keep_id}")
            print(f"🗑️  Will delete: {', '.join(delete_ids)}")
            print()
            
            confirm = input("   Proceed with deletion? (yes/no): ").strip().lower()
            
            if confirm == 'yes':
                result = await cleanup_duplicate_accounts(keep_id, delete_ids)
                print()
                print("=" * 70)
                print("✅ CLEANUP COMPLETE")
                print("=" * 70)
                print(f"   Kept: {result['kept']}")
                print(f"   Deleted: {result['deleted']}")
                print(f"   Failed: {result['failed']}")
            else:
                print("❌ Cleanup cancelled")
        else:
            print("❌ Invalid account ID")
    else:
        print("✅ No LinkedIn duplicates found")
    
    # Step 3: Audit database
    print()
    print("=" * 70)
    print("🔍 Step 3: Auditing database...")
    print("=" * 70)
    await audit_client_accounts()
    
    print()
    print("=" * 70)
    print("✅ ALL DONE!")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())