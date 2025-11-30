import os
from dotenv import load_dotenv
from database import MappingDatabase
from deel_client import DeelClient

# Load environment variables
load_dotenv(dotenv_path='.env')

DEEL_API_KEY = os.getenv('DEEL_API_KEY')


def review_pending_matches():
    """Interactive script to review and approve/reject uncertain matches."""
    db = MappingDatabase()
    deel = DeelClient(DEEL_API_KEY)

    pending = db.get_pending_reviews()

    if not pending:
        print("\n✓ No matches pending review - all done!")
        return

    print(f"\n{'=' * 80}")
    print(f"You have {len(pending)} matches that need your review")
    print(f"{'=' * 80}\n")

    approved_count = 0
    rejected_count = 0

    for i, mapping in enumerate(pending, 1):
        print(f"\n{'─' * 80}")
        print(f"Match {i} of {len(pending)}")
        print(f"{'─' * 80}")
        print(f"\n📊 Confidence: {mapping['confidence_score']:.1%}")
        print(f"\n🌱 Harvest User:")
        print(f"   Name:  {mapping['harvest_name']}")
        print(f"   Email: {mapping['harvest_email']}")
        print(f"   ID:    {mapping['harvest_user_id']}")
        print(f"\n💼 Deel Contract:")
        print(f"   Name:  {mapping['deel_name']}")
        print(f"   Email: {mapping['deel_email']}")
        print(f"   ID:    {mapping['deel_contract_id']}")

        # Show match signals for debugging
        import json
        signals = json.loads(mapping['match_signals'])
        print(f"\n🔍 Match Details:")
        if 'email_match' in signals:
            print(f"   Email Match: {'✓ Yes' if signals['email_match'] == 1.0 else '✗ No'}")
        if 'name_similarity' in signals:
            print(f"   Name Similarity: {signals['name_similarity']:.1%}")

        print(f"\n{'─' * 80}")
        choice = input("Do these match? (y=approve, n=reject, s=skip): ").lower().strip()

        if choice == 'y':
            db.verify_mapping(
                mapping['harvest_user_id'],
                approved=True,
                verified_by="manual_review"
            )

            # Also set external_id in Deel for future lookups
            success = deel.set_external_id(
                mapping['deel_contract_id'],
                mapping['harvest_user_id']
            )

            if success:
                print("✅ Approved and external_id set in Deel")
                approved_count += 1
            else:
                print("✅ Approved (but failed to set external_id - check Deel API)")
                approved_count += 1

        elif choice == 'n':
            db.verify_mapping(
                mapping['harvest_user_id'],
                approved=False,
                verified_by="manual_review"
            )
            print("❌ Rejected - this mapping will be deactivated")
            rejected_count += 1

        else:
            print("⏭️  Skipped - will remain in review queue")

    print(f"\n{'=' * 80}")
    print(f"Review Complete!")
    print(f"{'=' * 80}")
    print(f"✅ Approved: {approved_count}")
    print(f"❌ Rejected: {rejected_count}")
    print(f"⏭️  Skipped: {len(pending) - approved_count - rejected_count}")
    print(f"{'=' * 80}\n")


def show_all_mappings():
    """Display all current mappings."""
    db = MappingDatabase()
    mappings = db.get_all_mappings()

    print(f"\n{'=' * 80}")
    print(f"All User Mappings ({len(mappings)} total)")
    print(f"{'=' * 80}\n")

    for mapping in mappings:
        status_icon = {
            'auto_matched': '🤖',
            'human_verified': '✅',
            'needs_review': '❓'
        }.get(mapping['verification_status'], '❔')

        print(f"{status_icon} {mapping['harvest_name'][:25]:25} ↔️  {mapping['deel_name'][:25]:25} "
              f"({mapping['confidence_score']:.0%})")

    print(f"\n{'=' * 80}\n")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == '--list':
        show_all_mappings()
    else:
        review_pending_matches()