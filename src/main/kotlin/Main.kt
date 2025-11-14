
import org.example.DataManipulation
import org.example.DataProcess
import java.util.Scanner

fun main() {
    val scanner = Scanner(System.`in`)
    var choice: Int? = null

    while (choice != 3) {
        println("Select Language Implementation")
        println("[1] Load the file")
        println("[2] Generate Reports")
        println("[3] Exit")
        print("Enter your choice: ")

        if (scanner.hasNextInt()) {
            choice = scanner.nextInt()

            when (choice) {
                1 -> {
                    val prepare = DataManipulation()
                    val process = DataProcess()
                    prepare.prepareData()
                    process.loadFile()
                }
                2 -> {
                    println("You selected Option B.")
                    // Add functionality for Option B here
                }
                3 -> {
                    println("Exiting the program. Goodbye!")
                }
                else -> {
                    println("Invalid choice. Please enter a number between 1 and 3.")
                }
            }
        } else {
            println("Invalid input. Please enter a number.")
            scanner.next()
        }
        println("\n")
    }
    scanner.close()
}
