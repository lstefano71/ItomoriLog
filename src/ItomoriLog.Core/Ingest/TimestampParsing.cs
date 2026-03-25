using System.Globalization;

namespace ItomoriLog.Core.Ingest;

public static class TimestampParsing
{
    private static readonly string[] TwoDigitYearFormats =
    [
        "yy-MM-dd HH:mm:ss.fff",
        "yy-MM-dd HH:mm:ss",
        "yy/MM/dd HH:mm:ss.fff",
        "yy/MM/dd HH:mm:ss",
        "MM/dd/yy HH:mm:ss.fff",
        "MM/dd/yy HH:mm:ss",
        "dd/MM/yy HH:mm:ss.fff",
        "dd/MM/yy HH:mm:ss"
    ];

    public static readonly Calendar WindowedTwoDigitYearCalendar = CreateWindowedTwoDigitYearCalendar();

    public static bool TryParseWithTwoDigitYearWindow(
        string text,
        out DateTime parsed,
        out bool usedTwoDigitYear)
    {
        parsed = default;
        usedTwoDigitYear = false;

        var dtfi = (DateTimeFormatInfo)CultureInfo.InvariantCulture.DateTimeFormat.Clone();
        dtfi.Calendar = WindowedTwoDigitYearCalendar;

        if (DateTime.TryParseExact(
            text,
            TwoDigitYearFormats,
            dtfi,
            DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.NoCurrentDateDefault,
            out parsed)) {
            usedTwoDigitYear = true;
            return true;
        }

        return false;
    }

    private static Calendar CreateWindowedTwoDigitYearCalendar()
    {
        var calendar = new GregorianCalendar();
        // YY <= 49 => 20xx, YY >= 50 => 19xx
        // Achieved by TwoDigitYearMax=2049.
        calendar.TwoDigitYearMax = 2049;
        return calendar;
    }
}
