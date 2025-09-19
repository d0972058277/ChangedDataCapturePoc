using MongoChangeStreamSeeder.Domain;
using MongoDB.Bson;
using MongoDB.Driver;

var mongoConnection = Environment.GetEnvironmentVariable("MONGO_CONNECTION_STRING")
                      ?? "mongodb://localhost:37027/inventory?replicaSet=rs0&directConnection=true";

var databaseName = Environment.GetEnvironmentVariable("MONGO_DATABASE") ?? "inventory";
var collectionName = Environment.GetEnvironmentVariable("MONGO_COLLECTION") ?? "wagers";

var client = new MongoClient(mongoConnection);
var database = client.GetDatabase(databaseName);
var collection = database.GetCollection<Wager>(collectionName);

var random = new Random();
var wagerPrefix = args.Length > 0 ? args[0] : "WAGER";
var confirmedCount = 0;
var canceledCount = 0;
var pendingCount = 0;

for (var i = 0; i < 10; i++)
{
    var now = DateTime.UtcNow;
    var wagerId = $"{wagerPrefix}-{now:yyyyMMddHHmmss}-{random.Next(1000, 9999)}";
    var gameId = $"GAME-{random.Next(1, 100)}";
    var sessionId = $"SESSION-{Guid.NewGuid():N}";
    var userId = $"USER-{random.Next(1, 5000):D4}";

    var wager = Wager.Create(wagerId, gameId, sessionId, userId);

    wager.SetCreationInfo($"txn-{Guid.NewGuid():N}", now);

    var betAmount = Math.Round((decimal)(random.NextDouble() * 100), 2);
    var betTransactionId = $"txn-{Guid.NewGuid():N}";
    wager.Bet(betTransactionId, now.AddSeconds(5), betAmount);

    var outcomeTransactionId = $"txn-{Guid.NewGuid():N}";
    var outcomeTime = now.AddSeconds(20);
    var playerWon = random.Next(0, 2) == 0;

    if (playerWon)
    {
        var winAmount = Math.Round(betAmount * (decimal)(1 + random.NextDouble()), 2);
        wager.Win(outcomeTransactionId, outcomeTime, winAmount);
    }
    else
    {
        wager.Lose(outcomeTransactionId, outcomeTime, betAmount);
    }

    var shouldFinalize = random.Next(0, 10) >= 3;

    if (shouldFinalize)
    {
        var finalTransactionId = $"txn-{Guid.NewGuid():N}";
        var finalTimestamp = now.AddMinutes(1);
        var confirmed = random.Next(0, 2) == 0;

        if (confirmed)
        {
            wager.Confirm(finalTransactionId, finalTimestamp);
            confirmedCount++;
        }
        else
        {
            wager.Cancel(finalTransactionId, finalTimestamp);
            canceledCount++;
        }
    }
    else
    {
        pendingCount++;
    }

    await collection.InsertOneAsync(wager);

    Console.WriteLine($"Inserted wager #{i + 1} to {databaseName}.{collectionName}");
    Console.WriteLine(wager.ToJson(new MongoDB.Bson.IO.JsonWriterSettings { Indent = true }));
    Console.WriteLine($"IsCompleted: {wager.IsCompleted()}");
}

Console.WriteLine($"Confirm count: {confirmedCount}");
Console.WriteLine($"Cancel count: {canceledCount}");
Console.WriteLine($"Pending count: {pendingCount}");
