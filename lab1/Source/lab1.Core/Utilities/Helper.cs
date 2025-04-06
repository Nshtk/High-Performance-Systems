using System;
using System.Collections.Generic;
using System.Linq;

public static class Helper
{
	public static T GetRandomItem<T>(IEnumerable<T> items_as_enumerable, Func<T, int> getWeight)
	{
		Random random = new Random();
		List<T> items = items_as_enumerable.ToList();
		int weight_total = items.Sum(x => getWeight(x));
		int index_random_weighted = random.Next(weight_total);
		int index_item_weighted = 0;

		foreach (var item in items)
		{
			index_item_weighted += getWeight(item);
			if (index_random_weighted < index_item_weighted)
				return item;
		}
		throw new ArgumentException("Collection count and weights must be greater than 0");
	}
}