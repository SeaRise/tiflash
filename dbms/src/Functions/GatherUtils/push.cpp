#include <Functions/GatherUtils/Algorithms.h>
#include <Functions/GatherUtils/Selectors.h>

namespace DB::GatherUtils
{
struct ArrayPush : public ArrayAndValueSourceSelectorBySink<ArrayPush>
{
    template <typename ArraySource, typename ValueSource, typename Sink>
    static void selectArrayAndValueSourceBySink(
        ArraySource && array_source,
        ValueSource && value_source,
        Sink && sink,
        bool push_front)
    {
        if (push_front)
            concat(value_source, array_source, sink);
        else
            concat(array_source, value_source, sink);
    }
};


void push(IArraySource & array_source, IValueSource & value_source, IArraySink & sink, bool push_front)
{
    ArrayPush::select(sink, array_source, value_source, push_front);
}
} // namespace DB::GatherUtils
