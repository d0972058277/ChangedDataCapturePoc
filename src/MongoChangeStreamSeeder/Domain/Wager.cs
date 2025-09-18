using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MongoChangeStreamSeeder.Domain;

/// <summary>
/// Simplified wager aggregate for seeding and reporting demos.
/// </summary>
public sealed class Wager
{
    [BsonId]
    public string Id { get; private set; }

    [BsonElement("gameId")]
    public string GameId { get; private set; }

    [BsonElement("gameSessionId")]
    public string GameSessionId { get; private set; }

    [BsonElement("userId")]
    public string UserId { get; private set; }

    [BsonElement("version")]
    public long Version { get; private set; }

    [BsonElement("totalBet")]
    [BsonRepresentation(BsonType.Decimal128)]
    public decimal TotalBet { get; private set; }

    [BsonElement("totalWin")]
    [BsonRepresentation(BsonType.Decimal128)]
    public decimal TotalWin { get; private set; }

    [BsonElement("totalLose")]
    [BsonRepresentation(BsonType.Decimal128)]
    public decimal TotalLose { get; private set; }

    [BsonElement("events")]
    public List<WagerEvent> Events { get; private set; } = new();

    [BsonElement("beginTime")]
    [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
    public DateTime? BeginTime { get; private set; }

    [BsonElement("endTime")]
    [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
    public DateTime? EndTime { get; private set; }

    [BsonElement("lastUpdateTime")]
    [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
    public DateTime? LastUpdateTime { get; private set; }

    // Required for MongoDB deserialization
    private Wager()
    {
        Id = string.Empty;
        GameId = string.Empty;
        GameSessionId = string.Empty;
        UserId = string.Empty;
    }

    private Wager(string id, string gameId, string gameSessionId, string userId)
        : this()
    {
        Id = id;
        GameId = gameId;
        GameSessionId = gameSessionId;
        UserId = userId;
    }

    public static Wager Create(string wagerId, string gameId, string gameSessionId, string userId)
    {
        return new Wager(wagerId, gameId, gameSessionId, userId);
    }

    public void SetCreationInfo(string transactionId, DateTime utcNow)
    {
        BeginTime = utcNow;
        AppendEvent(new WagerCreate(transactionId, utcNow));
    }

    public void Bet(string transactionId, DateTime occurredAtUtc, decimal amount)
    {
        TotalBet += amount;
        AppendEvent(new WagerBet(transactionId, occurredAtUtc, amount));
    }

    public void Win(string transactionId, DateTime occurredAtUtc, decimal amount)
    {
        TotalWin += amount;
        AppendEvent(new WagerWin(transactionId, occurredAtUtc, amount));
    }

    public void Lose(string transactionId, DateTime occurredAtUtc, decimal amount)
    {
        TotalLose += amount;
        AppendEvent(new WagerLose(transactionId, occurredAtUtc, amount));
    }

    public void Confirm(string transactionId, DateTime executedAtUtc)
    {
        AppendEvent(new WagerConfirm(transactionId, executedAtUtc));
        EndTime = executedAtUtc;
    }

    public void Cancel(string transactionId, DateTime executedAtUtc)
    {
        AppendEvent(new WagerCancel(transactionId, executedAtUtc));
        EndTime = executedAtUtc;
    }

    /// <summary>
    /// Completed when the last persisted event represents confirmation or cancellation.
    /// </summary>
    public bool IsCompleted()
    {
        var last = Events.LastOrDefault();
        return last is WagerConfirm or WagerCancel;
    }

    private void AppendEvent(WagerEvent evt)
    {
        Events.Add(evt);
        Version++;
        LastUpdateTime = evt.OccurredAtUtc;
    }
}

/// <summary>
/// Base type for wager events serialized to MongoDB.
/// </summary>
[BsonDiscriminator(RootClass = true)]
[BsonKnownTypes(typeof(WagerCreate), typeof(WagerBet), typeof(WagerWin), typeof(WagerLose), typeof(WagerConfirm), typeof(WagerCancel))]
public abstract record WagerEvent(string TransactionId, DateTime OccurredAtUtc);

public sealed record WagerCreate(string TransactionId, DateTime OccurredAtUtc) : WagerEvent(TransactionId, OccurredAtUtc);

public sealed record WagerBet(string TransactionId, DateTime OccurredAtUtc, [property: BsonRepresentation(BsonType.Decimal128)] decimal Amount) : WagerEvent(TransactionId, OccurredAtUtc);

public sealed record WagerWin(string TransactionId, DateTime OccurredAtUtc, [property: BsonRepresentation(BsonType.Decimal128)] decimal Amount) : WagerEvent(TransactionId, OccurredAtUtc);

public sealed record WagerLose(string TransactionId, DateTime OccurredAtUtc, [property: BsonRepresentation(BsonType.Decimal128)] decimal Amount) : WagerEvent(TransactionId, OccurredAtUtc);

public sealed record WagerConfirm(string TransactionId, DateTime OccurredAtUtc) : WagerEvent(TransactionId, OccurredAtUtc);

public sealed record WagerCancel(string TransactionId, DateTime OccurredAtUtc) : WagerEvent(TransactionId, OccurredAtUtc);


