// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
import { expect, test } from "@jest/globals";
// --8<-- [start:versioning_imports]
import * as lancedb from "@lancedb/lancedb";
// --8<-- [end:versioning_imports]
import { withTempDirectory } from "./util.ts";

test("versioning examples", async () => {
  await withTempDirectory(async (databaseDir) => {
    // --8<-- [start:versioning_basic_setup]
    // Connect to LanceDB
    const db = await lancedb.connect(databaseDir);

    // Create a table with initial data
    const data = [
      { id: 1, author: "Richard", quote: "Wubba Lubba Dub Dub!" },
      { id: 2, author: "Morty", quote: "Rick, what's going on?" },
      { id: 3, author: "Richard", quote: "I turned myself into a pickle, Morty!" },
    ];

    const table = await db.createTable("quotes_versioning_example", data, {
      mode: "overwrite",
    });
    // --8<-- [end:versioning_basic_setup]

    // --8<-- [start:versioning_check_initial_version]
    // View the initial version
    const versions = await table.listVersions();
    console.log(`Number of versions after creation: ${versions.length}`);
    console.log(`Current version: ${await table.version()}`);
    // --8<-- [end:versioning_check_initial_version]
    expect(versions.length).toBe(1);

    // --8<-- [start:versioning_update_data]
    // Update author names to be more specific
    await table.update({
      where: "author = 'Richard'",
      values: { author: "Richard Daniel Sanchez" },
    });
    const rowsAfterUpdate = await table.countRows();
    console.log(`Number of rows after update: ${rowsAfterUpdate}`);
    // --8<-- [end:versioning_update_data]

    // --8<-- [start:versioning_add_data]
    // Add more data
    const moreData = [
      { id: 4, author: "Richard Daniel Sanchez", quote: "That's the way the news goes!" },
      { id: 5, author: "Morty", quote: "Aww geez, Rick!" },
    ];
    await table.add(moreData);
    // --8<-- [end:versioning_add_data]

    // --8<-- [start:versioning_check_versions_after_mod]
    // Check versions after modifications
    const versionsAfterMod = await table.listVersions();
    const versionAfterMod = await table.version();
    console.log(`Number of versions after modifications: ${versionsAfterMod.length}`);
    console.log(`Current version: ${versionAfterMod}`);
    // --8<-- [end:versioning_check_versions_after_mod]

    // --8<-- [start:versioning_list_all_versions]
    // Let's see all versions
    const allVersions = await table.listVersions();
    for (const v of allVersions) {
      console.log(`Version ${v.version}, created at ${v.timestamp}`);
    }
    // --8<-- [end:versioning_list_all_versions]

    // --8<-- [start:versioning_rollback]
    // Roll back to a previous version
    await table.checkout(versionAfterMod);
    await table.restore();

    // Notice we have one more version now, not less!
    const versionsAfterRollback = await table.listVersions();
    console.log(`Total number of versions after rollback: ${versionsAfterRollback.length}`);
    // --8<-- [end:versioning_rollback]

    // --8<-- [start:versioning_checkout_latest]
    // Go back to the latest version
    await table.checkoutLatest();
    // --8<-- [end:versioning_checkout_latest]

    // --8<-- [start:versioning_delete_data]
    // Delete data from the table
    await table.delete("author != 'Richard Daniel Sanchez'");
    const rowsAfterDeletion = await table.countRows();
    console.log(`Number of rows after deletion: ${rowsAfterDeletion}`);
    // --8<-- [end:versioning_delete_data]
  });
});
