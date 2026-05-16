export type AttrColumn = {
  name: string;
  ty: string;
};

/** 在已有 DEFATTR 的表上追加一列（StructDB MDB：`ADDATTR(name:type)`）。 */
export function buildAddAttrCommand(addName: string, addType: string): string {
  return `ADDATTR(${addName.trim()}:${addType.trim()})`;
}
