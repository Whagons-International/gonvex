// Builds the spreadsheet fixtures the K20 and L09 tests share (cloudflare/sandbox/test/fixtures):
//   node scripts/make-fixtures.mjs        (from cloudflare/spikes/l09-sandbox, after pnpm install)
// workbook.xlsx is written here, by hand, so every cell kind is present
// (shared, inline and empty strings, numbers, booleans, an error, a date,
// an offset range, an empty row, an empty sheet). workbook.xls is LibreOffice's
// BIFF8 conversion of it (soffice must be on PATH).
import { execFileSync } from "node:child_process";
import { copyFileSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { strToU8, zipSync } from "fflate";

const out = new URL("../../../sandbox/test/fixtures/", import.meta.url).pathname;
const xml = (body) => `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n${body}`;
const shared = ["Region", "Amount", "Closed", "Note", "Day", "north", "first & <one>", "south", "east", "", "k", "v", "a", "b"];
const s = (ref, text) => `<c r="${ref}" t="s"><v>${shared.indexOf(text)}</v></c>`;
const n = (ref, value, style = "") => `<c r="${ref}"${style}><v>${value}</v></c>`;
const b = (ref, value) => `<c r="${ref}" t="b"><v>${value ? 1 : 0}</v></c>`;
const sheet1 = xml(`<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><dimension ref="B2:F6"/><sheetData>
<row r="2">${s("B2", "Region")}${s("C2", "Amount")}${s("D2", "Closed")}${s("E2", "Note")}${s("F2", "Day")}</row>
<row r="3">${s("B3", "north")}${n("C3", 7)}${b("D3", true)}${s("E3", "first &amp; <one>".replace("&amp;", "&").replace("<", "<"))}${n("F3", 45292, ' s="1"')}</row>
<row r="4">${s("B4", "south")}${n("C4", 4.5)}${b("D4", false)}<c r="E4" t="inlineStr"><is><t>x</t></is></c>${n("F4", 45293, ' s="1"')}</row>
<row r="5"><c r="B5" s="1"/></row>
<row r="6">${s("B6", "east")}<c r="C6" t="e"><v>#DIV/0!</v></c>${b("D6", true)}${s("E6", "")}</row>
</sheetData></worksheet>`);
const sheet2 = xml(`<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData/></worksheet>`);
const sheet3 = xml(`<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>
<row r="1">${s("A1", "k")}${s("B1", "v")}</row><row r="2">${s("A2", "a")}${n("B2", 1)}</row><row r="3">${s("A3", "b")}${n("B3", 0.1)}</row>
</sheetData></worksheet>`);
const escape = (t) => t.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
const files = {
  "[Content_Types].xml": strToU8(xml(`<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/worksheets/sheet2.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/worksheets/sheet3.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/></Types>`)),
  "_rels/.rels": strToU8(xml(`<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>`)),
  "xl/workbook.xml": strToU8(xml(`<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Sales 2024" sheetId="1" r:id="rId1"/><sheet name="Empty" sheetId="2" r:id="rId2"/><sheet name="2nd" sheetId="3" r:id="rId3"/></sheets></workbook>`)),
  "xl/_rels/workbook.xml.rels": strToU8(xml(`<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/><Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet3.xml"/><Relationship Id="rId4" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/><Relationship Id="rId5" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/></Relationships>`)),
  "xl/styles.xml": strToU8(xml(`<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><fonts count="1"><font/></fonts><fills count="1"><fill/></fills><borders count="1"><border/></borders><cellStyleXfs count="1"><xf/></cellStyleXfs><cellXfs count="2"><xf numFmtId="0"/><xf numFmtId="14" applyNumberFormat="1"/></cellXfs></styleSheet>`)),
  "xl/sharedStrings.xml": strToU8(xml(`<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="${shared.length}" uniqueCount="${shared.length}">${shared.map((t) => (t === "" ? "<si><t/></si>" : `<si><t xml:space="preserve">${escape(t)}</t></si>`)).join("")}</sst>`)),
  "xl/worksheets/sheet1.xml": strToU8(sheet1),
  "xl/worksheets/sheet2.xml": strToU8(sheet2),
  "xl/worksheets/sheet3.xml": strToU8(sheet3),
};
writeFileSync(join(out, "workbook.xlsx"), zipSync(files, { mtime: new Date("2026-01-01T00:00:00Z") }));
const work = mkdtempSync(join(tmpdir(), "k20-fixtures-"));
try {
  copyFileSync(join(out, "workbook.xlsx"), join(work, "workbook.xlsx"));
  execFileSync("soffice", ["--headless", `-env:UserInstallation=file://${work}/profile`, "--convert-to", "xls", "workbook.xlsx"], { cwd: work, stdio: "ignore" });
  copyFileSync(join(work, "workbook.xls"), join(out, "workbook.xls"));
} finally {
  rmSync(work, { recursive: true, force: true });
}
// Tests run inside workerd, which has no file system: they import the bytes from this module.
const b64 = (name) => readFileSync(join(out, name)).toString("base64");
writeFileSync(join(out, "workbooks.ts"), `// Generated by scripts/make-fixtures.mjs from workbook.xlsx and workbook.xls. Do not edit.\nexport const WORKBOOK_XLSX = "${b64("workbook.xlsx")}";\nexport const WORKBOOK_XLS = "${b64("workbook.xls")}";\n`);
console.log("wrote test/fixtures/workbook.xlsx, workbook.xls and workbooks.ts");
