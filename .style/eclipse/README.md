# Eclipse IDE

This document describes how the Eclipse IDE can be setup.

## Checkstyle

1. Install Checkstyle from: http://eclipse-cs.sourceforge.net/

1. Add the Checkstyle configuration, which can be found [here](../checkstyle.xml):
   1. Go to Window > Preferences > Checkstyle;
   1. Press New...;
   1. At 'Type:' select 'Project Relative Configuration';
   1. 'Name:' it 'CARML Checks';
   1. At 'Location:' Browse... to `checkstyle.xml`;
   1. At 'Advanced options' check 'Protect Checkstyle configuration file';
   1. Press OK;
   1. Select the new 'CARML Checks' check configuration and make it the default by pressing 'Set as Default'.

   For more info about the Google Java code style see:
   https://google.github.io/styleguide/javaguide.html

   Changes to the Google Java code style:
   * Javadoc is not required on public methods. Method names should be descriptive by themselves. Only add Javadoc if necessary;
   * Some abbreviatons (like `IRI`) are allowed;
   * You can suppress Checkstyle warnings with:

    ```
    @SuppressWarnings("checkstyle:<check-name>")
    ```

    ...for example:

    ```
    @SuppressWarnings("checkstyle:LineLength")
    @SuppressWarnings({"checkstyle:MemberName", "checkstyle:ParamName"})
    ```
    Note that we do not encourage the usage of @SuppressWarnings. Use with care!

## Formatter

1. Activate the CarmlStyle formatter profile:
   1. Go to Window > Preferences > Java > Code Style > Formatter;
   1. Press Import... and import the CarmlStyle profile from [eclipse-java-style-carml.xml](eclipse-java-style-carml.xml);
   1. Press OK.

1. Organize Imports:
   1. Go to Window > Preferences > Java > Code Style > Organize Imports;
   1. Press Import... and import the CarmlStyle impor order from [carml.importorder](carml.importorder);
   1. Press OK.

1. Set the default Save Actions:
   1. Go to Window > Preferences > Java > Editor > Save Actions;
   1. Check 'Perform the selected actions on save';
   1. Check 'Format source code';
   1. Check 'Organize imports'.

1. Ignore unhandled token in @SuppressWarnings (i.e. "checkstyle:*"):
   1. Go to Window > Preferences > Java > Compiler > Errors/Warnings > Annotations;
   1. Set 'Unhandled token in '@SuppressWarnings'' to Ignore.

## SonarLint

1. Install Sonarlint from http://www.sonarlint.org/eclipse/;

1. After installing any issues will be reported as Eclipse markers.
