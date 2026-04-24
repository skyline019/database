# Rust+Vue GUI Test Cases

This file provides an end-to-end test checklist with sample schemas/data.

## 1) Test Workspace Preparation

1. Launch `newdb-rust-vue-gui.exe`.
2. Set workspace to a clean folder, e.g. `C:/tmp/newdb_gui_case`.
3. Open `DLL 状态` and ensure `已加载`.

## 2) Sample Schema and Data

Run in command box:

```text
CREATE TABLE(hr.employees)
USE(hr.employees)
DEFATTR(name:string,dept:string,age:int,salary:int,join_date:string)
SET PRIMARY KEY(id)
INSERT(1,Alice,ENG,29,18000,2024-01-10)
INSERT(2,Bob,ENG,33,22000,2023-07-01)
INSERT(3,Carol,HR,31,16000,2022-03-20)
INSERT(4,David,FIN,40,26000,2021-11-05)
```

Create a second table:

```text
CREATE TABLE(sales.orders)
USE(sales.orders)
DEFATTR(customer:string,amount:int,status:string,order_date:string)
INSERT(1,Acme,1200,paid,2025-02-11)
INSERT(2,Zen,3300,pending,2025-02-12)
INSERT(3,Bee,2400,paid,2025-02-13)
```

## 3) Tree Structure Tests

- Verify left tree has schema nodes: `hr`, `sales`.
- Expand/collapse works for each schema node.
- Table leaf nodes show table icon.
- Right-click menu appears only on leaf nodes.
- Breadcrumb updates after table switch.

## 4) Grid/Table View Tests

- Headers render as grid columns with borders.
- Body cells have both vertical and horizontal grid lines.
- Zebra stripe appears on even rows.
- Hover row highlight works.
- Horizontal scrollbar appears when columns overflow.
- Vertical scrollbar appears with many rows.

## 5) Pagination/Sort Tests

On `hr.employees`:

```text
PAGE(1,2,id,asc)
PAGE(2,2,id,asc)
PAGE(1,2,salary,desc)
```

Expected:
- page 1/2 content differs.
- desc by salary shows David/Bob first.

## 6) Query/Aggregation Tests

```text
WHERE(dept,=,ENG)
COUNT()
SUM(salary)
AVG(salary)
MIN(age)
MAX(age)
```

Expected:
- WHERE returns Alice/Bob.
- COUNT=4 for employees table.

## 7) CRUD Tests

```text
INSERT(5,Eva,ENG,27,15000,2025-01-01)
UPDATE(5,Eva,ENG,28,15500,2025-01-01)
FIND(5)
DELETE(5)
FIND(5)
```

Expected:
- First FIND exists, second FIND not found.

## 8) Export/Import Tests

```text
EXPORT CSV employees.csv
EXPORT JSON employees.json
```

- Verify files are generated in workspace.
- Create new workspace and run:

```text
IMPORTDIR(C:/tmp/newdb_gui_case)
SHOW TABLES
```

Expected imported tables appear.

## 9) Transaction Tests

```text
BEGIN
INSERT(6,Fiona,HR,30,17000,2025-03-03)
ROLLBACK
FIND(6)
BEGIN
INSERT(7,Gary,FIN,36,21000,2025-03-03)
COMMIT
FIND(7)
```

Expected:
- id=6 absent after rollback.
- id=7 exists after commit.

## 10) Undo/Redo Strategy Tests

- Execute reversible actions from menus (e.g. create table / insert data).
- Click `撤销` to run inverse command.
- In redo stack, click `编辑重做`, modify command, then execute.
- Verify edited command takes effect (not original).

## 11) Menu/Overlay Tests

- Open top menu and move mouse into submenu: menu must remain visible.
- Ensure submenu is not covered by main panel/raw area.
- Click outside: menu should close.

## 12) Help/Settings Tests

- Open help dialog; search keywords `insert`, `事务`, `export`.
- Verify matched text is highlighted.
- Open settings:
  - change accent,
  - switch gradient/image mode,
  - set image URL,
  - change font scale/dense mode.
- Restart app and confirm settings persisted.

## 13) Automated Regression Loop (2026-04-21)

Executed in ASCII workspace `C:/tmp/newdb_gui_build` with looped run/fix/retest.

- Scenario A: multi-column table render (`qa_regression`) - PASS
  - Commands: `CREATE TABLE`, `DEFATTR(name:string,dept:string,age:int,salary:int)`, 4x `INSERT`
  - Result: `PAGE(1,2,id,asc)` and `PAGE(2,2,id,asc)` return grid rows and page split is correct.
- Scenario B: sort switch - PASS
  - Command: `PAGE(1,2,salary,desc)`
  - Result: first page is salary-desc ordered (`David`, `Bob` first).
- Scenario C: empty-table pagination (`qa_empty_reg`) - PASS (expected empty)
  - Commands: `CREATE TABLE`, `DEFATTR(name:string,score:int)`, then `PAGE(1,12,id,asc)`
  - Backend output is `[PAGE] invalid page_no=1, total_pages=0`, and GUI fallback must render normal Vue table area with empty rows (not raw overlay block).
- Scenario D: layout overlap check - PASS
  - Main table region no longer overlays pager and command input section after CSS grid/flex fix.

Regression closure criteria for this loop:

- Table area always renders as Vue grid container (`headers + rows` path).
- Even when backend returns raw/empty-page text, UI keeps table region stable and non-overlapping.
