package edu.upenn.streamstesting.examples.mapreduce;

// import org.apache.flink.api.common.functions;
// 
// /**
//  * Common set of items for the examples.
//  */
// 
// public interface ReducerExamplesItem extends Serializable {
//     Integer key;
//     Integer x;
//     Integer y;
// }
// 
// /**
//  * 1. SingleItem reducer
//  * Requires the functional dependency key -> x.
//  */
// public class SingleItemReducer implements
//     GroupReduceFunction<ReducerExamplesItem, Integer>
// {
//     @Override
//     public void reduce(Iterable<ReducerExamplesItem> in,
//                        Collector<Integer> out) {
//         Integer x = null;
//         for (ReducerExamplesItem i: in) {
//             x = i.x;
//         }
//         if (x != null) {
//             out.collect(x);
//         }
//     }
// }
// 
// /**
//  * 2. IndexValuePair reducer
//  * Requires the functional dependency (key, x) -> y.
//  */
// public class IndexValuePairReducer implements
//     GroupReduceFunction<ReducerExamplesItem, Map<Integer, Integer>>
// {
//     @Override
//     public void reduce(Iterable<ReducerExamplesItem> in,
//                        Collector<Map<Integer, Integer>> out) {
//         Map<Integer, Integer> outmap = new HashMap<Integer, Integer>();
//         for (ReducerExamplesItem i: in) {
//             Integer x = in.x;
//             Integer y = in.y;
//             outmap.put(x, y)
//         }
//         out.collect(outmap.clone())
//     }
// }
// 
// /**
//  * 3. MaxRowReducer
//  * Requires either:
//  *   - the functional dependency (key, x) -> y
//  *   - OR there is at most one item with the max value of x
//  */
// public class MaxRowReducer implements
//     GroupReduceFunction<ReducerExamplesItem, Pair<Integer, Integer>>
// {
//     @Override
//     public void reduce(Iterable<ReducerExamplesItem> in,
//                        Collector<Pair<Integer, Integer>> out) {
//         Map<Integer, Integer> outmap = new HashMap<Integer, Integer>();
//         Integer max_x = 0;
//         Integer corresponding_y = null;
//         for (ReducerExamplesItem i: in) {
//             Integer x = in.x;
//             if (x > max_x) {
//                 max_x = x;
//                 corresponding_y = in.y;
//             }
//         }
//         out.collect(new Pair<Integer, Integer> (max_x, corresponding_y));
//     }
// }
// 
// /**
//  * 4. FirstNReducer
//  * Requires that there are at most 100 items to process.
//  * Otherwise, nondeterministic for most inputs.
//  */
// public class FirstNReducer implements
//     GroupReduceFunction<ReducerExamplesItem, Set<ReducerExamplesItem>>
// {
//     @Override
//     public void reduce(Iterable<ReducerExamplesItem> in,
//                        Collector<Set<ReducerExamplesItem>> out) {
//         Set<ReducerExamplesItem> items = new HashSet<ReducerExamplesItem>();
//         Integer count = 0;
//         for (ReducerExamplesItem i: in) {
//             count++;
//             if (count > 100) {
//                 break;
//             }
//             items.add(i);
//         }
//         out.collect(items);
//     }
// }
// 
// /**
//  * 5. StrConcatReducer
//  * Requires that the consumer of the output does not care about order.
//  */
// public class StrConcatReducer implements
//     GroupReduceFunction<ReducerExamplesItem, ReducerExamplesItem>
// {
//     @Override
//     public void reduce(Iterable<ReducerExamplesItem> in,
//                        Collector<ReducerExamplesItem> out) {
//         for (ReducerExamplesItem i: in) {
//             out.collect(i);
//         }
//     }
// }
