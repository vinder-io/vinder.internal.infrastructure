namespace Vinder.Internal.Infrastructure.Persistence;

public static class FilterDefinitions
{
    public static FilterDefinition<BsonDocument> MatchIfNotEmpty(string field, string? parameter)
    {
        return string.IsNullOrWhiteSpace(parameter)
            ? FilterDefinition<BsonDocument>.Empty
            : Builders<BsonDocument>.Filter.Eq(field, BsonValue.Create(parameter));
    }

    public static FilterDefinition<BsonDocument> MatchIfNotEmpty(string field, DateTime? parameter)
    {
        return parameter.HasValue
            ? Builders<BsonDocument>.Filter.Eq(field, parameter.Value)
            : FilterDefinition<BsonDocument>.Empty;
    }

    public static FilterDefinition<BsonDocument> MatchIfNotEmpty(string field, DateOnly? parameter)
    {
        return parameter.HasValue
            ? Builders<BsonDocument>.Filter.Eq(field, parameter.Value.ToDateTime(TimeOnly.MinValue))
            : FilterDefinition<BsonDocument>.Empty;
    }

    public static FilterDefinition<BsonDocument> MatchIfNotEmptyContains(string field, string? parameter)
    {
        return string.IsNullOrWhiteSpace(parameter)
            ? FilterDefinition<BsonDocument>.Empty
            : Builders<BsonDocument>.Filter.Regex(field, new BsonRegularExpression(parameter, "i"));
    }

    public static FilterDefinition<BsonDocument> MatchBool(string field, bool? parameter, bool defaultValue = false)
    {
        return Builders<BsonDocument>.Filter.Eq(field, parameter ?? defaultValue);
    }

    public static FilterDefinition<BsonDocument> MatchIfNotEmptyEnum<TEnum>(string field, TEnum? parameter)
        where TEnum : struct, Enum
    {
        return parameter.HasValue
            ? Builders<BsonDocument>.Filter.Eq(field, Convert.ToInt32(parameter.Value))
            : FilterDefinition<BsonDocument>.Empty;
    }
}