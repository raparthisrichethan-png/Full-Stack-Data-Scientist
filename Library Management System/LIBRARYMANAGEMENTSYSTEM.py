import os
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load .env
load_dotenv()
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# ---------------- CRUD OPERATIONS ---------------- #

# --- Members ---
def add_member(name, email):
    payload = {"name": name, "email": email}
    return supabase.table("members").insert(payload).execute().data

def list_members():
    return supabase.table("members").select("*").execute().data or []

def update_member(member_id, new_email):
    return supabase.table("members").update({"email": new_email}).eq("member_id", member_id).execute().data

def delete_member(member_id):
    # only if no borrowed books
    borrowed = supabase.table("borrow_records").select("*").eq("member_id", member_id).is_("return_date", None).execute().data or []
    if borrowed:
        return {"error": "❌ Member has borrowed books, cannot delete!"}
    return supabase.table("members").delete().eq("member_id", member_id).execute().data

# --- Books ---
def add_book(title, author, category, stock):
    payload = {"title": title, "author": author, "category": category, "stock": stock}
    return supabase.table("books").insert(payload).execute().data

def list_books():
    return supabase.table("books").select("*").execute().data or []

def update_book_stock(book_id, new_stock):
    return supabase.table("books").update({"stock": new_stock}).eq("book_id", book_id).execute().data

def delete_book(book_id):
    borrowed = supabase.table("borrow_records").select("*").eq("book_id", book_id).is_("return_date", None).execute().data or []
    if borrowed:
        return {"error": "❌ Book is currently borrowed, cannot delete!"}
    return supabase.table("books").delete().eq("book_id", book_id).execute().data

# ---------------- SEARCH ---------------- #

def search_books(keyword):
    keyword = keyword.strip()
    if not keyword:
        return []

    seen = set()
    results = []

    for field in ("title", "author", "category"):
        resp = supabase.table("books").select("*").ilike(field, f"%{keyword}%").execute()
        rows = resp.data or []
        for r in rows:
            bid = r.get("book_id")
            if bid not in seen:
                seen.add(bid)
                results.append(r)
    return results

# ---------------- TRANSACTIONS ---------------- #

def borrow_book(member_id, book_id):
    # check stock
    book_resp = supabase.table("books").select("stock").eq("book_id", book_id).single().execute()
    book = book_resp.data
    if not book or book.get("stock", 0) <= 0:
        return {"error": "❌ Book not available!"}

    try:
        supabase.table("books").update({"stock": book["stock"] - 1}).eq("book_id", book_id).execute()
        record_resp = supabase.table("borrow_records").insert({
            "member_id": member_id,
            "book_id": book_id
        }).execute()
        return {"success": "✅ Book borrowed!", "record": record_resp.data}
    except Exception as e:
        return {"error": f"Transaction failed: {e}"}

def return_book(record_id):
    rec_resp = supabase.table("borrow_records").select("*").eq("record_id", record_id).single().execute()
    record = rec_resp.data
    if not record:
        return {"error": "❌ Record not found!"}
    if record.get("return_date"):
        return {"error": "❌ Book already returned!"}

    try:
        supabase.table("borrow_records").update({"return_date": datetime.utcnow().isoformat()}).eq("record_id", record_id).execute()
        book_resp = supabase.table("books").select("stock").eq("book_id", record["book_id"]).single().execute()
        book = book_resp.data or {"stock": 0}
        supabase.table("books").update({"stock": (book.get("stock", 0) + 1)}).eq("book_id", record["book_id"]).execute()
        return {"success": "✅ Book returned!"}
    except Exception as e:
        return {"error": f"Transaction failed: {e}"}

# ---------------- REPORTS ---------------- #

def report_top_books():
    try:
        resp = supabase.rpc("top_books").execute()
        return resp.data or []
    except Exception:
        return []

def report_overdue():
    overdue_limit = datetime.utcnow() - timedelta(days=14)
    records = supabase.table("borrow_records") \
        .select("record_id, member_id, book_id, borrow_date") \
        .is_("return_date", None) \
        .lt("borrow_date", overdue_limit.isoformat()) \
        .execute().data or []
    return records

def report_member_borrows():
    try:
        resp = supabase.rpc("member_borrow_count").execute()
        return resp.data or []
    except Exception:
        return []

# ---------------- MAIN CLI ---------------- #
def print_book(b):
    print(f"[{b.get('book_id')}] {b.get('title')} — {b.get('author')} ({b.get('category')}) | stock: {b.get('stock')}")

def print_member(m):
    print(f"[{m.get('member_id')}] {m.get('name')} — {m.get('email')} (joined {m.get('join_date')})")

def main():
    while True:
        print("\n📚 Library Management System")
        print("1. Add Member")
        print("2. Add Book")
        print("3. Search Books")
        print("4. List Books")
        print("5. Update Book Stock")
        print("6. Update Member Email")
        print("7. Delete Book")
        print("8. Delete Member")
        print("9. Borrow Book")
        print("10. Return Book")
        print("11. Reports")
        print("12. Exit")

        choice = input("Enter choice: ").strip()

        if choice == "1":
            name = input("Enter member name: ").strip()
            email = input("Enter member email: ").strip()
            print(add_member(name, email))

        elif choice == "2":
            title = input("Enter title: ").strip()
            author = input("Enter author: ").strip()
            category = input("Enter category: ").strip()
            stock = int(input("Enter stock: ").strip())
            print(add_book(title, author, category, stock))

        elif choice == "3":
            keyword = input("Search keyword: ").strip()
            results = search_books(keyword)
            if not results:
                print("No matches found.")
            else:
                for b in results:
                    print_book(b)

        elif choice == "4":
            for b in list_books():
                print_book(b)

        elif choice == "5":
            bid = int(input("Enter Book ID: ").strip())
            new_stock = int(input("Enter new stock: ").strip())
            print(update_book_stock(bid, new_stock))

        elif choice == "6":
            mid = int(input("Enter Member ID: ").strip())
            new_email = input("Enter new email: ").strip()
            print(update_member(mid, new_email))

        elif choice == "7":
            bid = int(input("Enter Book ID: ").strip())
            print(delete_book(bid))

        elif choice == "8":
            mid = int(input("Enter Member ID: ").strip())
            print(delete_member(mid))

        elif choice == "9":
            mid = int(input("Member ID: ").strip())
            bid = int(input("Book ID: ").strip())
            print(borrow_book(mid, bid))

        elif choice == "10":
            rid = int(input("Borrow record ID: ").strip())
            print(return_book(rid))

        elif choice == "11":
            print("Reports")
            print("1. Top borrowed books")
            print("2. Overdue books")
            print("3. Borrow count per member")
            rch = input("Choose: ").strip()
            if rch == "1":
                print(report_top_books())
            elif rch == "2":
                print(report_overdue())
            elif rch == "3":
                print(report_member_borrows())

        elif choice == "12":
            print("Goodbye!")
            break

        else:
            print("Invalid choice")
if __name__ == "__main__":
    main()