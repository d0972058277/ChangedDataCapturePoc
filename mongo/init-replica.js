const config = {
  _id: "rs0",
  members: [{ _id: 0, host: "mongodb:37017" }],
};

function initiateReplicaSet() {
  try {
    const result = rs.initiate(config);
    printjson(result);
  } catch (error) {
    if (error.codeName === "AlreadyInitialized" ||
        error.code === 23 ||
        (typeof error.errmsg === "string" && error.errmsg.includes("already initialized"))) {
      print("Replica set already initialized");
    } else {
      throw error;
    }
  }
}

function waitForPrimary(maxAttempts, delayMs) {
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    try {
      const status = rs.status();
      if (status.myState === 1) {
        print("Replica set primary is ready");
        return;
      }
      print(`Replica set state: ${status.myState}, waiting for PRIMARY`);
    } catch (statusError) {
      print(`Replica set status check failed: ${statusError}`);
    }
    sleep(delayMs);
  }
  throw new Error("Replica set did not reach PRIMARY state in time");
}

function ensureCollections(maxAttempts, delayMs) {
  const inventoryDb = db.getSiblingDB("inventory");
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    try {
      inventoryDb.createCollection("products");
    } catch (collectionError) {
      const message = collectionError.message || collectionError.errmsg || String(collectionError);
      if (message.includes("NamespaceExists")) {
        // collection already exists
      } else if (message.includes("not primary")) {
        print(`Waiting for primary before creating collection (attempt ${attempt + 1})`);
        sleep(delayMs);
        continue;
      } else {
        print(`Collection init warning: ${collectionError}`);
      }
   }

   try {
      inventoryDb.products.createIndex({ sku: 1 }, { unique: true });
   } catch (indexError) {
      const idxMessage = indexError.message || indexError.errmsg || String(indexError);
      if (!idxMessage.includes("already exists")) {
        throw indexError;
      }
   }

    print("inventory.products collection ready");
    return;
  }

  throw new Error("Failed to initialize inventory.products collection");
}

initiateReplicaSet();
waitForPrimary(40, 1000);
ensureCollections(10, 1000);
