package com.signomix.common.billing;

public class ValueToTextConverter {

    /**
     * Converts value to text representation in Polish.
     * Warning: maximum value that can be converted is 9999.99.
     * 
     * @param value The value to convert.
     * @return The text representation of the value.
     */
    public static String getValueAsText(double value) {
        if (value == 0) {
            return "zero złotych";
        }else if(value >=10000){
            return Double.toString(value) + " złotych";
        }

        StringBuilder result = new StringBuilder();
        long zloty = (long) value;
        int grosze = (int) Math.round((value - zloty) * 100);

        result.append(thousandsToWords(zloty));
        zloty %= 1000;
        result.append(hundredsToWords(zloty));
        zloty %= 100;
        result.append(tensToWords(zloty));
        zloty %= 10;
        result.append(numberToWords(zloty));
        if (zloty == 1) {
            result.append("złoty");
        } else if (zloty > 1 && zloty < 5) {
            result.append("złote");
        } else {
            result.append("złotych");
        }

        if (grosze > 0) {
            result.append(" i ").append(tensToWords(grosze));
            grosze %= 10;
            result.append(numberToWords(grosze));  
            if (grosze == 1) {
                result.append("grosz");
            } else if (grosze > 1 && grosze < 5) {
                result.append("grosze");
            } else {
                result.append("groszy");
            }
        }

        return result.toString();
    }

    /**
     * Converts a number to its word representation in Polish.
     * This is a simplified version and needs to be expanded for full support.
     * 
     * @param number The number to convert.
     * @return The word representation of the number.
     */
    private static String numberToWords(long number) {
        // This method needs to be implemented to fully convert numbers to words.
        // The following is a very simplified placeholder implementation.
        if (number > 19) {
            return "";
        }
        switch ((int) number) {
            case 1:
                return "jeden ";
            case 2:
                return "dwa ";
            case 3:
                return "trzy ";
            case 4:
                return "cztery ";
            case 5:
                return "pięć ";
            case 6:
                return "sześć ";
            case 7:
                return "siedem ";
            case 8:
                return "osiem ";
            case 9:
                return "dziewięć ";
            case 10:
                return "dziesięć ";
            case 11:
                return "jedenaście ";
            case 12:
                return "dwanaście ";
            case 13:
                return "trzynaście ";
            case 14:
                return "czternaście ";
            case 15:
                return "piętnaście ";
            case 16:
                return "szesnaście ";
            case 17:
                return "siedemnaście ";
            case 18:
                return "osiemnaście ";
            case 19:
                return "dziewiętnaście ";
            default:
                return "";
        }
    }

    private static  String tensToWords(long number) {
        // This method needs to be implemented to fully convert tens to words.
        // The following is a very simplified placeholder implementation.
        if (number >= 20 && number < 30) {
            return "dwadzieścia ";
        } else if (number >= 30 && number < 40) {
            return "trzydzieści ";
        } else if (number >= 40 && number < 50) {
            return "czterdzieści ";
        } else if (number >= 50 && number < 60) {
            return "pięćdziesiąt ";
        } else if (number >= 60 && number < 70) {
            return "sześćdziesiąt ";
        } else if (number >= 70 && number < 80) {
            return "siedemdziesiąt ";
        } else if (number >= 80 && number < 90) {
            return "osiemdziesiąt ";
        } else if (number >= 90 && number < 100) {
            return "dziewięćdziesiąt ";
        }
        return "";
    }

    private static String hundredsToWords(long number) {
        // This method needs to be implemented to fully convert hundreds to words.
        // The following is a very simplified placeholder implementation.
        if (number >= 100 && number < 200) {
            return "sto ";
        } else if (number >= 200 && number < 300) {
            return "dwieście ";
        } else if (number >= 300 && number < 400) {
            return "trzysta ";
        } else if (number >= 400 && number < 500) {
            return "czterysta ";
        } else if (number >= 500 && number < 600) {
            return "pięćset ";
        } else if (number >= 600 && number < 700) {
            return "sześćset ";
        } else if (number >= 700 && number < 800) {
            return "siedemset ";
        } else if (number >= 800 && number < 900) {
            return "osiemset ";
        } else if (number >= 900 && number < 1000) {
            return "dziewięćset ";
        }
        return "";
    }

    private static String thousandsToWords(long number) {
        // This method needs to be implemented to fully convert thousands to words.
        // The following is a very simplified placeholder implementation.
        if (number >= 1000 && number < 2000) {
            return "tysiąc ";
        } else if (number >= 2000 && number < 3000) {
            return "dwa tysiące ";
        } else if (number >= 3000 && number < 4000) {
            return "trzy tysiące ";
        } else if (number >= 4000 && number < 5000) {
            return "cztery tysiące ";
        } else if (number >= 5000 && number < 6000) {
            return "pięć tysięcy ";
        } else if (number >= 6000 && number < 7000) {
            return "sześć tysięcy ";
        } else if (number >= 7000 && number < 8000) {
            return "siedem tysięcy ";
        } else if (number >= 8000 && number < 9000) {
            return "osiem tysięcy ";
        } else if (number >= 9000 && number < 10000) {
            return "dziewięć tysięcy ";
        }
        return "";
    }

    public static void main(String[] args) {
        System.out.println(getValueAsText(Double.parseDouble(args[0])));
    }   
}
