import org.apache.hadoop.io.Text;


public class UtilsFunctions {

    /**
     * Reverses the order of words based on the number of input words.
     */
    public static Text reverseTextWithSpaces(Text input) {
        String[] words = input.toString().split(" ");
        int length = words.length;

        StringBuilder reversed = new StringBuilder();

        switch (length) {
            case 3:
                reversed.append(words[2]).append(" ").append(words[1]).append(" ").append(words[0]);
                break;
            case 2:
                reversed.append(words[1]).append(" ").append(words[0]);
                break;
            case 1:
                reversed.append(words[0]);
                break;
            default:
                System.out.println("UtilsFunctions.reverseWords: Invalid input format" + input.toString());
            }

            return new Text(reversed.toString());
        }
}
