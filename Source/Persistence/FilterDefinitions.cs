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

    public static FilterDefinition<BsonDocument> MatchIfContains(string field, string? parameter)
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

    public static FilterDefinition<BsonDocument> MustBeWithinIfNotNull(string field, DateOnly? min = null, DateOnly? max = null)
    {
        if (!min.HasValue && !max.HasValue)
            return FilterDefinition<BsonDocument>.Empty;

        var filters = new List<FilterDefinition<BsonDocument>>();

        if (min.HasValue)
            filters.Add(Builders<BsonDocument>.Filter.Gte(field, min.Value));

        if (max.HasValue)
            filters.Add(Builders<BsonDocument>.Filter.Lte(field, max.Value));

        return Builders<BsonDocument>.Filter.And(filters);
    }

    public static FilterDefinition<BsonDocument> MustBeWithinIfNotNull(string field, decimal? min = null, decimal? max = null)
    {
        if (!min.HasValue && !max.HasValue)
            return FilterDefinition<BsonDocument>.Empty;

        var filters = new List<FilterDefinition<BsonDocument>>();

        if (min.HasValue)
            filters.Add(Builders<BsonDocument>.Filter.Gte(field, min.Value));

        if (max.HasValue)
            filters.Add(Builders<BsonDocument>.Filter.Lte(field, max.Value));

        return Builders<BsonDocument>.Filter.And(filters);
    }

    public static FilterDefinition<BsonDocument> MustBeWithinIfNotNull(string field, int? min = null, int? max = null)
    {
        if (!min.HasValue && !max.HasValue)
            return FilterDefinition<BsonDocument>.Empty;

        var filters = new List<FilterDefinition<BsonDocument>>();

        if (min.HasValue)
            filters.Add(Builders<BsonDocument>.Filter.Gte(field, min.Value));

        if (max.HasValue)
            filters.Add(Builders<BsonDocument>.Filter.Lte(field, max.Value));

        return Builders<BsonDocument>.Filter.And(filters);
    }

    public static FilterDefinition<BsonDocument> MustBeInIfNotEmpty<T>(string field, IReadOnlyCollection<T>? values)
    {
        if (values == null || values.Count == 0)
            return FilterDefinition<BsonDocument>.Empty;

        return Builders<BsonDocument>.Filter.In(field, values);
    }

    public static FilterDefinition<BsonDocument> MatchIfNotEmptyDictionary(string field, Dictionary<string, object>? values)
    {
        if (values == null || values.Count == 0)
            return FilterDefinition<BsonDocument>.Empty;

        var filters = values
            .Select(pair => Builders<BsonDocument>.Filter.Eq($"{field}.{pair.Key}", BsonValue.Create(pair.Value)))
            .ToList();

        return Builders<BsonDocument>.Filter.And(filters);
    }
}