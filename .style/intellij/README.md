# IntelliJ IDE

This document describes how the IntelliJ IDE can be setup.

## General

1. Navigate to Settings... > Editor > General.

1. Check "Ensure every saved file ends with a line break".

1. Install Lombok plugin from Settings... > Plugins.

## Eclipse Code Formatter

1. Install Eclipse Code Formatter plugin from Settings... > Plugins.

1. Navigate to Settings... > Other Settings > Eclipse Code Formatter.

1. Select "Use the Eclipse code formatter".

1. Select "New profile" and rename the new profile to "CarmlStyle".

1. Under "Eclipse Java Formatter config file", import [eclipse-java-style-carml.xml](../eclipse/eclipse-java-style-carml.xml)

1. Select "Optimize imports" and import [carml.importorder](../eclipse/carml.importorder).

1. Formatting can now be easily done with `Ctrl-Alt-L`.

## Checkstyle

1. Install Checkstyle plugin from Settings... > Plugins.

1. Navigate to Settings... > Tools > Checkstyle.

1. Import and activate the [checkstyle.xml](../checkstyle.xml) from the `.style` folder. Select the corresponding Checkstyle version.

## SonarLint

1. Install Sonarlint plugin from Settings... > Plugins.

1. Navigate to Settings... > Other Settings > SonarLint General Settings.

1. Add the SonarQube server (SonarCloud). Select the `carml` organization.

1. Navigate to Settings... > Other Settings > SonarLint Project Settings.

1. Bind the previously created SonarQube server and select the right project.
